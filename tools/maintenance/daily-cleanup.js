(async function cleanupFiles() {
  const fs = require('fs');
  const path = require('path');
  
  try {
    logger.info('开始执行文件清理任务...');
    
    // 清理超过3天的日志备份文件（.gz格式）
    const logsPath = '/vertex/logs';
    const threeDaysAgo = Date.now() - (3 * 24 * 60 * 60 * 1000);
    
    if (fs.existsSync(logsPath)) {
      const logFiles = fs.readdirSync(logsPath);
      let deletedLogFiles = 0;
      
      for (const file of logFiles) {
        if (file.endsWith('.gz')) {
          const filePath = path.join(logsPath, file);
          try {
            const stats = fs.statSync(filePath);
            
            if (stats.mtime.getTime() < threeDaysAgo) {
              fs.unlinkSync(filePath);
              deletedLogFiles++;
              logger.info(`删除日志备份文件: ${file}`);
            }
          } catch (fileError) {
            logger.warn(`处理日志文件 ${file} 时出错: ${fileError.message}`);
          }
        }
      }
      logger.info(`清理了 ${deletedLogFiles} 个超过3天的日志备份文件`);
    } else {
      logger.warn('日志目录不存在: ' + logsPath);
    }
    
    // 清理超过3天的种子文件
    const torrentsPath = '/vertex/torrents';
    
    if (fs.existsSync(torrentsPath)) {
      const torrentFiles = fs.readdirSync(torrentsPath);
      let deletedTorrentFiles = 0;
      
      for (const file of torrentFiles) {
        if (file.endsWith('.torrent')) {
          const filePath = path.join(torrentsPath, file);
          try {
            const stats = fs.statSync(filePath);
            
            if (stats.mtime.getTime() < threeDaysAgo) {
              fs.unlinkSync(filePath);
              deletedTorrentFiles++;
              logger.info(`删除种子文件: ${file}`);
            }
          } catch (fileError) {
            logger.warn(`处理种子文件 ${file} 时出错: ${fileError.message}`);
          }
        }
      }
      logger.info(`清理了 ${deletedTorrentFiles} 个超过3天的种子文件`);
    } else {
      logger.warn('种子目录不存在: ' + torrentsPath);
    }
    
    logger.info('文件清理任务完成');
    
  } catch (error) {
    logger.error('文件清理任务执行失败:', error);
  }
});