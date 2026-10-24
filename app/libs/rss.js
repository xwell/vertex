const parser = require('xml2js').parseString;
const crypto = require('crypto');
const moment = require('moment');
const path = require('path');
const fs = require('fs');
const bencode = require('bencode');
const util = require('./util');
const redis = require('./redis');
const logger = require('./logger');

const parseXml = util.promisify(parser);

const _getSum = function (a, b) {
  return a + b;
};

const normalizeTorrentUrl = function (torrentUrl) {
  if (!torrentUrl) return torrentUrl;
  return torrentUrl.replace(/&(?:amp|#38|#x26);/ig, '&');
};

exports.normalizeTorrentUrl = normalizeTorrentUrl;

const parseSizeText = function (sizeText) {
  const match = (sizeText || '').replace(/\u00a0/g, ' ').match(/(\d+(?:\.\d+)?)\s*(KiB|MiB|GiB|TiB|KB|MB|GB|TB)\b/i);
  if (!match) return 0;
  const map = {
    kib: 1024,
    mib: 1024 * 1024,
    gib: 1024 * 1024 * 1024,
    tib: 1024 * 1024 * 1024 * 1024,
    kb: 1000,
    mb: 1000 * 1000,
    gb: 1000 * 1000 * 1000,
    tb: 1000 * 1000 * 1000 * 1000
  };
  return parseFloat(match[1]) * map[match[2].toLowerCase()];
};

const _parseRssContent = async function (body) {
  const feed = await parseXml(body);
  const channel = feed?.rss?.channel?.[0];
  if (!channel || typeof channel !== 'object') {
    throw new Error('RSS响应缺少有效的 channel');
  }
  // 合法的空订阅不是抓取失败，所有站点适配器均可安全遍历。
  channel.item = channel.item || [];
  return feed;
};

// 返回已校验的 RSS 对象，只缓存有效 XML，不缓存错误页面。
const _getRssContent = async function (rssUrl, suffix = true) {
  const startTime = Date.now();
  const host = new URL(rssUrl).host;
  const cacheKey = `vertex:rss:${rssUrl}`;
  const cache = await redis.get(cacheKey);
  if (cache) {
    try {
      const feed = await _parseRssContent(cache);
      logger.debug(host, `RSS源获取完成: 缓存命中, 耗时=${Date.now() - startTime}ms`);
      return feed;
    } catch (error) {
      // 兼容升级前残留的无效缓存，本次立即重新请求，不再空等一个周期。
      await redis.del(cacheKey);
      logger.warn(host, 'RSS缓存无效，已清除并重新获取');
    }
  }

  let url = rssUrl;
  const isPig = rssUrl.includes('https://piggo.me/');
  const isKamept = rssUrl.includes('https://kamept.com/');
  if (suffix && !isPig) {
    if (isKamept) {
      const randomNum = Math.floor(Math.random() * 100000000);
      url += '&' + randomNum;
    } else {
      url += (rssUrl.indexOf('?') === -1 ? '?' : '&') + '____=' + Math.random();
    }
  }
  let res;
  if (rssUrl.includes('https://pt.soulvoice.club/') && global.runningSite?.SoulVoice) {
    res = await util.requestPromise({
      url,
      headers: {
        cookie: global.runningSite.SoulVoice.cookie
      }
    }, true);
  } else {
    res = await util.requestPromise(url, true);
  }
  if (res.statusCode < 200 || res.statusCode >= 300) {
    throw new Error(`RSS请求失败: HTTP ${res.statusCode}`);
  }
  let body = res.body;
  if (typeof body !== 'string' || !body.trim()) {
    throw new Error('RSS响应内容为空或不是文本');
  }
  if (body.indexOf('xml-viewer-style') !== -1) {
    const match = body.match(/<rss\b[\s\S]*<\/rss>/i);
    if (!match) throw new Error('RSS展示页面中未找到 RSS 内容');
    body = '<?xml version="1.0" encoding="utf-8"?>\n' + match[0];
  }
  const feed = await _parseRssContent(body);
  // 保留站点专用间隔；展示格式本身不应导致额外的 310 秒缓存。
  let cacheTime = ['lemon', 'hhanclub'].some(item => host.indexOf(item) !== -1) ? 150 : 40;
  if (host.indexOf('sharkpt') !== -1) {
    cacheTime = 310;
  }
  await redis.setWithExpire(cacheKey, body, cacheTime);
  logger.debug(host, `RSS源获取完成: 网络请求, 耗时=${Date.now() - startTime}ms, 缓存=${cacheTime}s`);
  return feed;
};

const _getTorrents = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].enclosure[0].$.length;
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.description = items[i].description ? items[i].description[0] : '';
    torrent.id = link.substring(link.indexOf('?id=') + 4);
    torrent.url = items[i].enclosure[0].$.url;
    torrent.hash = items[i].guid[0]._ || items[i].guid[0];
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsMTeam = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl, false);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].enclosure[0].$.length;
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.description = items[i].description ? items[i].description[0] : '';
    torrent.id = link.match(/\/(\d+)/)[1];
    torrent.url = items[i].enclosure[0].$.url;
    torrent.hash = items[i].guid[0]._ || items[i].guid[0];
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsPuTao = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    const size = items[i].title[0].match(/\[\d+\.\d+ [KMGT]B\]/)?.[0].match(/\d+\.\d+ [KMGT]B/)?.[0];
    const map = {
      KB: 1000,
      MB: 1000 * 1000,
      GB: 1000 * 1000 * 1000,
      TB: 1000 * 1000 * 1000 * 1000
    };
    const matchResult = size?.match(/(\d*\.\d*|\d*) (GB|MB|TB|KB)/);
    if (matchResult) {
      torrent.size = parseFloat(matchResult[1]) * map[matchResult[2]];
    } else {
      torrent.size = 0;
    }
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link.substring(0, link.indexOf('&passkey=')).replace('download', 'details');
    torrent.id = torrent.link.substring(link.indexOf('?id=') + 4);
    torrent.url = link;
    torrent.hash = items[i].guid[0]._ || items[i].guid[0];
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsFileList = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    const size = items[i].description[0].match(/Size: (\d*\.\d*|\d*) (GB|MB|TB|KB)/)[0];
    const map = {
      KB: 1000,
      MB: 1000 * 1000,
      GB: 1000 * 1000 * 1000,
      TB: 1000 * 1000 * 1000 * 1000
    };
    const regRes = size.match(/Size: (\d*\.\d*|\d*) (GB|MB|TB|KB)/);
    torrent.size = parseFloat(regRes[1]) * map[regRes[2]];
    torrent.name = items[i].title[0].replace(/\n/, ' ');
    const link = items[i].link[0].match(/https:\/\/filelist.io\/download\.php\?id=\d*/)[0].replace('download', 'details');
    torrent.link = link;
    torrent.id = link.substring(link.indexOf('?id=') + 4);
    torrent.hash = 'fakehash' + torrent.id + 'fakehash';
    torrent.url = items[i].link[0];
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsBeyondHD = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    const size = items[i].title[0].match(/(\d*\.\d*|\d*) (GiB|MiB|TiB|KiB)/)[0];
    const map = {
      KiB: 1024,
      MiB: 1024 * 1024,
      GiB: 1024 * 1024 * 1024,
      TiB: 1024 * 1024 * 1024 * 1024
    };
    const regRes = size.match(/(\d*\.\d*|\d*) (GiB|MiB|TiB|KiB)/);
    torrent.size = parseFloat(regRes[1]) * map[regRes[2]];
    torrent.name = items[i].title[0].split('\n')[0];
    torrent.link = items[i].guid[0];
    torrent.id = torrent.link.match(/\.(\d+)/)[1];
    torrent.hash = 'fakehash' + torrent.id + 'fakehash';
    torrent.url = items[i].link[0];
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsUnit3D2 = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].contentlength ? parseInt(items[i].contentlength[0]) : parseSizeText(items[i].description && items[i].description[0]);
    torrent.name = items[i].title[0].trim();
    const link = items[i].link[0];
    torrent.id = (link.match(/download\/(\d+)\./) || [])[1] || items[i].guid[0];
    torrent.hash = 'fakehash' + torrent.id + 'fakehash';
    torrent.url = link;
    torrent.link = link.replace(/download\//, '').replace(/(\d+)\..*/, '$1');
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsUnit3D = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    const size = items[i].description[0].match(/Size<\/strong>: (\d*\.\d*|\d*) (GiB|MiB|TiB|KiB)/)[0];
    const map = {
      KiB: 1024,
      MiB: 1024 * 1024,
      GiB: 1024 * 1024 * 1024,
      TiB: 1024 * 1024 * 1024 * 1024
    };
    const regRes = size.match(/Size<\/strong>: (\d*\.\d*|\d*) (GiB|MiB|TiB|KiB)/);
    torrent.size = parseFloat(regRes[1]) * map[regRes[2]];
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.id = link.match(/torrents\/(\d*)/)[1];
    torrent.hash = 'fakehash' + torrent.id + 'fakehash';
    torrent.url = items[i].enclosure[0].$.url;
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsTorrentDB = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    const size = items[i].description[0].match(/(\d*\.\d*|\d*) (GB|MB|TB|KB)/)[0];
    const map = {
      KB: 1000,
      MB: 1000 * 1000,
      GB: 1000 * 1000 * 1000,
      TB: 1000 * 1000 * 1000 * 1000
    };
    const regRes = size.match(/(\d*\.\d*|\d*) (GB|MB|TB|KB)/);
    torrent.size = parseFloat(regRes[1]) * map[regRes[2]];
    torrent.name = items[i].title[0];
    const link = items[i].comments[0];
    torrent.link = link;
    torrent.hash = items[i].guid[0];
    torrent.url = items[i].persistentlink[0];
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsLuminance = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.id = link.substring(link.indexOf('?id=') + 4);
    torrent.url = items[i].enclosure[0].$.url;
    torrent.hash = items[i].torrent[0].infoHash[0];
    torrent.size = items[i].torrent[0].contentLength[0];
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsGazelle = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.name = items[i].title[0];
    const link = items[i].comments[0];
    torrent.link = link;
    torrent.url = items[i].link[0];
    torrent.id = +torrent.url.match(/id=(\d+)/)[1];
    const cache = await redis.get(`vertex:hash:${torrent.url}`);
    if (cache) {
      const _torrent = JSON.parse(cache);
      torrent.hash = _torrent.hash;
      torrent.size = _torrent.size;
    } else {
      try {
        const { exists, hash, size } = await exports.getTorrentNameByBencode(torrent.url);
        if (!exists) {
          continue;
        }
        torrent.hash = hash;
        torrent.size = size;
        await redis.set(`vertex:hash:${torrent.url}`, JSON.stringify(torrent));
      } catch (e) {
        await redis.set(`vertex:hash:${torrent.url}`, JSON.stringify({ hash: 'gzl' + moment().unix() + 'gzl', size: 0 }));
        throw e;
      }
    }
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  const allPubTimeSame = torrents.every(torrent => torrent.pubTime === torrents[0].pubTime);
  if (allPubTimeSame) {
    torrents.forEach(torrent => { delete torrent.pubTime; });
  }
  return torrents;
};

const _getTorrentsSkyeySnow = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].enclosure[0].$.length;
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.id = link.substring(link.indexOf('?id=') + 4);
    torrent.url = items[i].enclosure[0].$.url;
    if (torrent.url.indexOf('skyey') !== -1) {
      const cache = await redis.get(`vertex:hash:${torrent.url}`);
      if (cache) {
        torrent.hash = cache;
      } else {
        try {
          const { hash } = await exports.getTorrentNameByBencode(torrent.url);
          torrent.hash = hash;
          await redis.set(`vertex:hash:${torrent.url}`, hash);
        } catch (e) {
          await redis.set(`vertex:hash:${torrent.url}`, 'skyey' + moment().unix() + 'skyey');
          throw e;
        }
      }
    }
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsHDBits = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.name = items[i].title[0];
    const url = items[i].link[0];
    torrent.id = url.match(/id=(\d+)/)[1];
    torrent.url = url;
    torrent.link = `https://hdbits.org/details.php?id=${torrent.id}&source=browse`;
    if (torrent.url.indexOf('hdbits') !== -1) {
      const cache = await redis.get(`vertex:hash:${torrent.url}`);
      if (cache) {
        torrent.hash = cache;
      } else {
        try {
          const { hash, size } = await exports.getTorrentNameByBencode(torrent.url);
          torrent.hash = hash;
          torrent.size = size;
          await redis.set(`vertex:hash:${torrent.url}`, JSON.stringify(torrent));
        } catch (e) {
          await redis.set(`vertex:hash:${torrent.url}`, JSON.stringify({ hash: 'hdbits' + moment().unix() + 'hdbits', size: 0 }));
          throw e;
        }
      }
    }
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsHDTorrents = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.id = moment().unix();
    torrent.url = link;
    torrent.hash = link.match(/hash=(.*?)&/)[1];
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsHDCity = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].enclosure[0].$.length;
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.id = new URLSearchParams(link).get('t');
    torrent.link = `https://${new URL(rssUrl).host}/t-` + torrent.id;
    torrent.url = items[i].enclosure[0].$.url;
    torrent.hash = 'fakehash' + torrent.id + 'fakehash';
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsIPTorrents = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    const size = items[i].description[0].split(';')[0].replace('B', 'iB');
    torrent.size = util.calSize(...size.split(' '));
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.id = link.match(/\/(\d+)\//)[1];
    torrent.link = link;
    torrent.url = items[i].link[0];
    torrent.hash = 'fakehashipt' + torrent.id + 'fakehashipt';
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsMikanProject = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  if (!items) return [];
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].enclosure[0].$.length;
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.id = link.substring(link.indexOf('Episode/') + 8);
    torrent.url = items[i].enclosure[0].$.url;
    torrent.hash = torrent.id;
    torrent.pubTime = moment(items[i].torrent[0].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsLearnFlakes = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl, false);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].description[0].match(/\d+\.\d+ [MGKT]B/);
    if (torrent.size) {
      torrent.size = util.calSize(...torrent.size[0].replace(/([MGKT])B/, '$1iB').split(' '));
    } else {
      torrent.size = 0;
    }
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.id = link.match(/&tid=(\d+)/)[1];
    torrent.url = items[i].guid[0]._;
    torrent.hash = 'learnflakes' + torrent.id + 'learnflakes';
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsAvistaZ = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    const size = items[i].description[0].match(/Size<\/strong>: (\d*\.\d*|\d*) (GB|MB|TB|KB)/)[0].replace(/([GMTK])B/, '$1iB');
    const map = {
      KiB: 1024,
      MiB: 1024 * 1024,
      GiB: 1024 * 1024 * 1024,
      TiB: 1024 * 1024 * 1024 * 1024
    };
    const regRes = size.match(/Size<\/strong>: (\d*\.\d*|\d*) (GiB|MiB|TiB|KiB)/);
    torrent.size = parseFloat(regRes[1]) * map[regRes[2]];
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.id = link.match(/torrent\/(\d+)/)[1];
    torrent.url = items[i].enclosure[0].$.url;
    torrent.hash = items[i].guid[0]._.match(/-(.*)/)[1];
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsTorrentLeech = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    const guid = items[i].guid[0]._ || items[i].guid[0];
    torrent.name = items[i].title[0];
    torrent.url = items[i].link[0];
    torrent.link = guid;
    torrent.id = guid.substring(torrent.hash.indexOf('torrent/') + 8);
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    const cache = await redis.get(`vertex:hash:${torrent.url}`);
    if (cache) {
      const _torrent = JSON.parse(cache);
      torrent.hash = _torrent.hash;
      torrent.size = _torrent.size;
    } else {
      try {
        const { exists, hash, size } = await exports.getTorrentNameByBencode(torrent.url);
        if (!exists) {
          continue;
        }
        torrent.hash = hash;
        torrent.size = size;
        await redis.set(`vertex:hash:${torrent.url}`, JSON.stringify(torrent));
      } catch (e) {
        await redis.set(`vertex:hash:${torrent.url}`, JSON.stringify({ hash: 'tl' + moment().unix() + 'tl', size: 0 }));
        throw e;
      }
    }
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsFSM = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].enclosure[0].$.length;
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.id = link.substring(link.indexOf('?tid=') + 5);
    torrent.url = items[i].enclosure[0].$.url;
    torrent.hash = items[i].guid[0]._ || items[i].guid[0];
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsHappyFappy = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].torrent[0].contentLength[0];
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.id = link.substring(link.indexOf('?id=') + 4);
    torrent.url = items[i].enclosure[0].$.url;
    torrent.hash = items[i].torrent[0].infoHash[0];
    torrent.hash = Buffer.from(unescape(torrent.hash), 'binary').toString('hex');
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsKimoji = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    const size = items[i].description[0].match(/Size<\/strong>: (\d*\.\d*|\d*).(GiB|MiB|TiB|KiB)/)[0];
    const map = {
      KiB: 1024,
      MiB: 1024 * 1024,
      GiB: 1024 * 1024 * 1024,
      TiB: 1024 * 1024 * 1024 * 1024
    };
    const regRes = size.match(/Size<\/strong>: (\d*\.\d*|\d*).(GiB|MiB|TiB|KiB)/);
    torrent.size = parseFloat(regRes[1]) * map[regRes[2]];
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.id = link.match(/download\/(\d*)\./)[1];
    torrent.hash = 'fakehash' + torrent.id + 'fakehash';
    torrent.url = link;
    torrent.link = link.replace(/\/torrent\/download\/(\d+).*/, '/torrents/$1');
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsFappaizuri = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl, false);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].description[0].match(/Size: (\d+\.\d+ [MGKT]B)/);
    if (torrent.size) {
      torrent.size = util.calSize(...torrent.size[1].replace(/([MGKT])B/, '$1iB').split(' '));
    } else {
      torrent.size = 0;
    }
    torrent.name = items[i].title[0];
    const url = items[i].link[0];
    torrent.id = url.match(/id=(\d+)/)[1];
    torrent.link = 'https://fappaizuri.me/torrents-details.php?id=' + torrent.id;
    torrent.url = url;
    torrent.hash = 'fappaizuri' + torrent.id + 'fappaizuri';
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsLusthive = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.size = items[i].torrent[0].contentLength[0];
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.link = link;
    torrent.description = items[i].description ? items[i].description[0] : '';
    torrent.id = link.substring(link.indexOf('?id=') + 4);
    torrent.url = items[i].enclosure[0].$.url;
    torrent.hash = items[i].guid[0]._ || items[i].guid[0];
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsCinematik = async function (rssUrl) {
  const rss = await _getRssContent(rssUrl);
  const torrents = [];
  const items = rss.rss.channel[0].item;
  for (let i = 0; i < items.length; ++i) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    // 从contentlength获取大小
    torrent.size = items[i].contentlength ? parseInt(items[i].contentlength[0]) : 0;
    torrent.name = items[i].title[0];
    const link = items[i].link[0];
    torrent.id = link.match(/\/download\/(\d+)\./)[1]; // 提取ID
    torrent.link = `https://cinematik.net/torrents/${torrent.id}`; // 生成种子页面链接
    torrent.url = link;
    torrent.hash = 'cinematik' + torrent.id + 'cinematik';
    torrent.pubTime = moment(items[i].pubDate[0]).unix();
    torrents.push(torrent);
  }
  return torrents;
};

const _getTorrentsWrapper = {
  'filelist.io': _getTorrentsFileList,
  'bitporn.eu': _getTorrentsUnit3D2,
  'blutopia.cc': _getTorrentsUnit3D2,
  'jptv.club': _getTorrentsUnit3D,
  'anime-no-index.com': _getTorrentsUnit3D2,
  'monikadesign.uk': _getTorrentsUnit3D2,
  'kimoji.club': _getTorrentsKimoji,
  'torrentdb.net': _getTorrentsTorrentDB,
  'uhdbits.org': _getTorrentsGazelle,
  'www.empornium.is': _getTorrentsLuminance,
  'www.empornium.sx': _getTorrentsLuminance,
  'www.pixelcove.me': _getTorrentsLuminance,
  'www.cathode-ray.tube': _getTorrentsLuminance,
  'www.skyey2.com': _getTorrentsSkyeySnow,
  'hdbits.org': _getTorrentsHDBits,
  'beyond-hd.me': _getTorrentsBeyondHD,
  'pt.sjtu.edu.cn': _getTorrentsPuTao,
  'hd-torrents.org': _getTorrentsHDTorrents,
  'hdcity.leniter.org': _getTorrentsHDCity,
  'hdcity.city': _getTorrentsHDCity,
  'iptorrents.com': _getTorrentsIPTorrents,
  'mikanani.me': _getTorrentsMikanProject,
  'learnflakes.net': _getTorrentsLearnFlakes,
  'exoticaz.to': _getTorrentsAvistaZ,
  'avistaz.to': _getTorrentsAvistaZ,
  'cinemaz.to': _getTorrentsAvistaZ,
  'privatehd.to': _getTorrentsAvistaZ,
  'rss.torrentleech.org': _getTorrentsTorrentLeech,
  'rss24h.torrentleech.org': _getTorrentsTorrentLeech,
  'fsm.name': _getTorrentsFSM,
  'api.fsm.name': _getTorrentsFSM,
  'www.happyfappy.org': _getTorrentsHappyFappy,
  'fearnopeer.com': _getTorrentsUnit3D2,
  'jpopsuki.eu': _getTorrentsGazelle,
  'dicmusic.com': _getTorrentsGazelle,
  'greatposterwall.com': _getTorrentsGazelle,
  'libble.me': _getTorrentsGazelle,
  'fappaizuri.me': _getTorrentsFappaizuri,
  'lusthive.org': _getTorrentsLusthive,
  'cinematik.net': _getTorrentsCinematik
};

exports.getTorrents = async function (rssUrl) {
  let host = '无效RSS地址';
  try {
    host = new URL(rssUrl).host;
    let torrents;
    if (host.indexOf('m-team') !== -1) {
      torrents = await _getTorrentsMTeam(rssUrl);
    } else if (_getTorrentsWrapper[host]) {
      torrents = await _getTorrentsWrapper[host](rssUrl);
    } else {
      torrents = await _getTorrents(rssUrl);
    }
    torrents.forEach(torrent => { torrent.url = normalizeTorrentUrl(torrent.url); });
    return torrents;
  } catch (e) {
    // 不把失败伪装成空订阅；仅暴露主机及脱敏信息，避免日志泄露 passkey。
    const message = String(e.message || e).replace(/https?:\/\/[^\s"'<>]+/gi, '[RSS URL]');
    const error = new Error(`${host} 获取 RSS 失败: ${message}`);
    if (e.code) error.code = e.code;
    logger.error(error.message);
    throw error;
  }
};

exports.getTorrentName = async function (url) {
  const res = await util.requestPromise({
    url: normalizeTorrentUrl(url),
    method: 'HEAD'
  });
  const dis = res.headers['content-disposition'];
  const filename = dis.substring(dis.indexOf('filename=') + 9);
  return decodeURIComponent(filename);
};

exports.getTorrentNameByBencode = async function (url) {
  const res = await util.requestPromise({
    url: normalizeTorrentUrl(url),
    method: 'GET',
    encoding: null
  });
  const contentType = res.headers['content-type'];
  if (contentType.includes('application/x-bittorrent')) {
    const buffer = Buffer.from(res.body, 'utf-8');
    const torrent = bencode.decode(buffer);
    const size = torrent.info.length || torrent.info.files.map(i => i.length).reduce(_getSum, 0);
    const fsHash = crypto.createHash('sha1');
    fsHash.update(bencode.encode(torrent.info));
    const md5 = fsHash.digest('md5');
    let hash = '';
    for (const v of md5) {
      hash += v < 16 ? '0' + v.toString(16) : v.toString(16);
    }
    const filepath = path.join(__dirname, '../../torrents', hash + '.torrent');
    fs.writeFileSync(filepath, buffer);
    return {
      exists: true,
      hash,
      size,
      name: torrent.info.name.toString()
    };
  } else {
    return {
      exists: false,
      hash: '',
      size: 0,
      name: ''
    };
  }
};
