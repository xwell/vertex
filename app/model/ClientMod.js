const fs = require('fs');
const path = require('path');
const Client = require('../common/Client');
const Rss = require('../common/Rss');
const logger = require('../libs/logger');

const util = require('../libs/util');
const bandwidth = require('../libs/bandwidth');
class ClientMod {
  add(options) {
    bandwidth.validate(options);
    const id = util.uuid.v4().split('-')[0];
    const clientSet = { ...options };
    clientSet.id = id;
    clientSet.deleteRules = clientSet.deleteRules || [];
    clientSet.sameServerClients = clientSet.sameServerClients || [];
    clientSet.sameServerClients.push(id);
    fs.writeFileSync(path.join(__dirname, '../data/client/', id + '.json'), JSON.stringify(clientSet, null, 2));
    if (global.runningClient[id]) global.runningClient[id].destroy();
    if (clientSet.enable) global.runningClient[id] = new Client(clientSet);
    return '添加下载器成功';
  };

  delete(options) {
    fs.unlinkSync(path.join(__dirname, '../data/client/', options.id + '.json'));
    if (global.runningClient[options.id]) global.runningClient[options.id].destroy();
    return '删除下载器成功';
  };

  modify(options) {
    // Validate before destroying a live client or writing a configuration file.
    bandwidth.validate(options);
    const clientSet = { ...options };
    clientSet.deleteRules = clientSet.deleteRules || [];
    clientSet.sameServerClients = clientSet.sameServerClients || [];
    if (global.runningClient[clientSet.id]) global.runningClient[options.id].destroy();
    if (clientSet.enable) global.runningClient[options.id] = new Client(clientSet);
    fs.writeFileSync(path.join(__dirname, '../data/client/', options.id + '.json'), JSON.stringify(clientSet, null, 2));
    return '修改下载器成功';
  };

  list() {
    const rssList = util.listRss();
    const doubanList = util.listDouban();
    const clientList = util.listClient();
    const watchList = util.listWatch();
    for (const client of clientList) {
      // RSS 任务引用详情（前端弹窗用，不阻止操作）
      client.usedByRss = rssList
        .filter(item => (item.clientArr || [item.client]).indexOf(client.id) !== -1)
        .map(item => ({ id: item.id, alias: item.alias, isOnly: (item.clientArr || [item.client]).length <= 1 }));
      client.usedByReseed = rssList
        .filter(item => (item.reseedClients || []).indexOf(client.id) !== -1)
        .map(item => ({ id: item.id, alias: item.alias }));
      // 豆瓣/监控引用保持禁止操作
      client.usedByDouban = doubanList.some(item => item.client === client.id);
      client.usedByWatch = watchList.some(item => item.downloader === client.id);
      client.used = !global.ignoreDependCheck && (client.usedByDouban || client.usedByWatch);
      client.status = !!(client.enable && global.runningClient[client.id] && global.runningClient[client.id].status && global.runningClient[client.id].maindata);
      if (client.status) {
        client.allTimeUpload = global.runningClient[client.id].maindata.allTimeUpload;
        client.allTimeDownload = global.runningClient[client.id].maindata.allTimeDownload;
        client.uploadSpeed = global.runningClient[client.id].maindata.uploadSpeed;
        client.downloadSpeed = global.runningClient[client.id].maindata.downloadSpeed;
        client.leechingCount = global.runningClient[client.id].maindata.leechingCount;
        client.seedingCount = global.runningClient[client.id].maindata.seedingCount;
      }
    }
    return clientList;
  };

  listMainInfo() {
    const clientList = util.listClient();
    const clientInfos = [];
    for (const client of clientList) {
      const c = {};
      c.id = client.id;
      c.alias = client.alias;
      c.enable = client.enable;
      c.autoDelete = client.autoDelete;
      c.clientUrl = client.clientUrl;
      client.status = !!(client.enable && global.runningClient[client.id] && global.runningClient[client.id].status && global.runningClient[client.id].maindata);
      c.status = client.status;
      if (client.status) {
        c.allTimeUpload = global.runningClient[client.id].maindata.allTimeUpload;
        c.allTimeDownload = global.runningClient[client.id].maindata.allTimeDownload;
        c.uploadSpeed = global.runningClient[client.id].maindata.uploadSpeed;
        c.downloadSpeed = global.runningClient[client.id].maindata.downloadSpeed;
        c.leechingCount = global.runningClient[client.id].maindata.leechingCount;
        c.seedingCount = global.runningClient[client.id].maindata.seedingCount;
      }
      clientInfos.push(c);
    }
    return clientInfos;
  }

  listTop10({ id }) {
    const top10 = [];
    const client = global.runningClient[id];
    if (!client) throw new Error('下载器未启用');
    const top10Torrents = client.maindata.torrents.sort((a, b) => b.uploadSpeed - a.uploadSpeed || b.downloadSpeed - a.downloadSpeed).slice(0, 10);
    for (const torrent of top10Torrents) {
      const t = { ...torrent };
      delete t.originProp;
      top10.push(t);
    }
    return top10;
  }

  async getSpeedPerTracker() {
    const clients = global.runningClient;
    const trackers = {};
    for (const clientId of Object.keys(clients)) {
      if (!clients[clientId].maindata) continue;
      for (const torrent of clients[clientId].maindata.torrents) {
        const _tracker = torrent.tracker || '错误状态';
        const tracker = _tracker.match(/.*?([^.]*\.[^.]*$)/)[1];
        if (!trackers[tracker]) trackers[tracker] = { upload: 0, download: 0 };
        trackers[tracker].upload += torrent.uploadSpeed;
        trackers[tracker].download += torrent.downloadSpeed;
      }
    }
    const trackerArr = Object.keys(trackers).map(i => {
      return {
        ...trackers[i],
        tracker: i
      };
    });
    return {
      trackerList: trackerArr,
      trackers: Object.keys(trackers)
    };
  };

  async getLogs(options) {
    const clients = global.runningClient;
    return await clients[options.client].getLogs();
  };

  // 从所有 RSS 任务中移除指定下载器的引用
  forceRemoveFromRss(clientId, replacementClientId) {
    const rssList = util.listRss();
    const modified = [];
    for (const rss of rssList) {
      let changed = false;
      const clientArr = rss.clientArr || [rss.client];
      // 检查 clientArr 是否包含该下载器
      if (clientArr.indexOf(clientId) !== -1) {
        if (clientArr.length <= 1 && replacementClientId) {
          // 唯一下载器：替换为用户选择的替代下载器
          rss.clientArr = [replacementClientId];
        } else {
          // 多个下载器：直接移除
          rss.clientArr = clientArr.filter(id => id !== clientId);
        }
        changed = true;
      }
      // 检查辅种客户端列表
      if ((rss.reseedClients || []).indexOf(clientId) !== -1) {
        rss.reseedClients = rss.reseedClients.filter(id => id !== clientId);
        changed = true;
      }
      if (changed) {
        fs.writeFileSync(path.join(__dirname, '../data/rss/', rss.id + '.json'), JSON.stringify(rss, null, 2));
        if (global.runningRss[rss.id]) global.runningRss[rss.id].destroy();
        if (rss.enable) global.runningRss[rss.id] = new Rss(rss);
        modified.push(rss.alias || rss.id);
        logger.info(`下载器解除关联: RSS任务 [${rss.alias || rss.id}] 已移除下载器 ${clientId}`);
      }
    }
    return modified;
  };
}

module.exports = ClientMod;
