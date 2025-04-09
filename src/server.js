// Confirming overwrite of existing file
const os = require('os');
const crypto = require('crypto');
const express = require('express');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const cors = require('cors');
const multer = require('multer');
const ffmpeg = require('fluent-ffmpeg');
const fs = require('fs');
const he = require('he');
const path = require('path');
const database = require('./database');
const EventSource = require('eventsource');
const rateLimit = require('express-rate-limit');
const sharp = require('sharp');
const { google } = require('googleapis');
const drive = google.drive('v3');
const stream = require('stream');
const app = express();

// ... [Previous code remains unchanged until the streaming section]

app.post('/start-stream', async (req, res) => {
  try {
    const { 
      rtmp_url, 
      stream_key, 
      loop, 
      title,
      videoPath,
      schedule_enabled, 
      schedule_start_enabled, 
      schedule_start, 
      schedule_duration 
    } = req.body;

    if (!videoPath) return sendError(res, 'Video tidak ditemukan');
    if (!title) return sendError(res, 'Judul belum diisi');

    const sourceFilePath = path.join(__dirname, 'uploads', videoPath);
    if (!fs.existsSync(sourceFilePath)) {
      return sendError(res, 'Video tidak ditemukan di server');
    }

    const streamFileName = generateRandomFileName() + path.extname(videoPath);
    const streamFilePath = path.join(__dirname, 'uploads', streamFileName);

    await fs.promises.copyFile(sourceFilePath, streamFilePath);

    const fullRtmpUrl = `${rtmp_url}/${stream_key}`;
    console.log('Starting stream:', { rtmp_url, title });

    // Handle penjadwalan streaming
    if (schedule_enabled === '1' && schedule_start_enabled === '1') {
      const startTime = new Date(schedule_start).getTime();
      const duration = schedule_duration ? parseInt(schedule_duration, 10) * 60 * 1000 : null;

      const containerData = {
        title,
        preview_file: path.basename(videoPath),
        stream_file: streamFileName,
        stream_key,
        stream_url: rtmp_url,
        loop_enabled: loop === 'true' ? 1 : 0,
        container_order: Date.now(),
        is_streaming: 1,
        schedule_enabled: 1,
        schedule_start_enabled: 1,
        schedule_start,
        schedule_duration: duration ? Math.floor(duration / 1000 / 60) : null
      };

      const result = await new Promise((resolve, reject) => {
        database.addStreamContainer(containerData, (err, data) => {
          if (err) reject(err);
          resolve(data);
        });
      });

      scheduleStream({
        videoPath: streamFilePath,
        stream_key,
        rtmp_url,
        containerId: result.lastID,
        loop
      }, startTime, duration);

      return res.json({ 
        message: 'Stream scheduled',
        scheduled: true,
        startTime,
        duration 
      });
    }

    // Modified ffmpeg command to remove encoding
    const command = ffmpeg(streamFilePath)
      .inputFormat('mp4')
      .inputOptions(['-re', ...(loop === 'true' ? ['-stream_loop -1'] : [])])
      .outputOptions([
        '-c:v copy',  // Copy video stream without re-encoding
        '-c:a copy',  // Copy audio stream without re-encoding
        '-f flv'      // Output format
      ])
      .output(`${rtmp_url}/${stream_key}`);

    const duration = parseInt(schedule_duration, 10) * 60 * 1000;
    if (schedule_enabled === '1' && duration) {
      setTimeout(() => {
        if (streams[stream_key]) {
          try {
            streams[stream_key].process.on('error', (err) => {
              if (err.message.includes('SIGTERM')) {
                return;
              }
              console.error('FFmpeg error:', err);
            });

            streams[stream_key].process.kill('SIGTERM');
            database.updateStreamContainer(
              streams[stream_key].containerId, 
              { is_streaming: 0, auto_stopped: true }, 
              (err) => {
                if (err) console.error('Error updating stream status:', err);
            });

            delete streams[stream_key];
          } catch (error) {
            console.error('Error stopping stream:', error);
          }
        }
      }, duration);
    }

    let responseSent = false;
    let containerId;

    try {
      const containerData = {
        title: title,
        preview_file: path.basename(videoPath),
        stream_file: streamFileName,
        stream_key: stream_key,
        stream_url: rtmp_url,
        loop_enabled: loop === 'true' ? 1 : 0,
        container_order: Date.now(),
        is_streaming: 1,
        schedule_enabled: parseInt(req.body.schedule_enabled, 10),
        schedule_start_enabled: parseInt(req.body.schedule_start_enabled, 10),
        schedule_duration_enabled: parseInt(req.body.schedule_duration_enabled, 10),
        schedule_start: req.body.schedule_start || null,
        schedule_duration: req.body.schedule_duration ? parseInt(req.body.schedule_duration, 10) : null
      };

      const result = await new Promise((resolve, reject) => {
        database.addStreamContainer(containerData, (err, data) => {
          if (err) {
            reject(new Error(`Database error: ${err.message}`));
            return;
          }
          resolve(data);
        });
      });
      containerId = result.lastID;

      if (!result) throw new Error("Gagal menyimpan data ke database");
      
      streams[stream_key] = {
        process: command,
        startTime: Date.now(),
        containerId: containerId,
        videoPath: streamFilePath,
        duration: duration
      };

      command
        .on('end', () => {
          console.log('Streaming selesai:', stream_key);
          const monitor = monitorStreams.get(stream_key);
          if (monitor) {
            monitor.isActive = false;
          }
          delete streams[stream_key];
          database.updateStreamContainer(containerId, { is_streaming: 0 }, (err) => {
            if (err) console.error('Error updating database:', err);
          });
        })
        .on('error', (err) => {
          console.error('Stream error:', err);
          delete streams[stream_key];
          if (!responseSent) {
            sendError(res, 'Error during streaming', 500);
            responseSent = true;
          }
        })
        .run();

      setTimeout(() => {
        if (!responseSent) {
          res.json({ message: 'Streaming dimulai', stream_key, containerId: containerId });
          responseSent = true;
        }
      }, 5000);
    } catch (error) {
      console.error('Error starting stream:', error);
      if (!responseSent) {
        sendError(res, `Failed to start stream: ${error.message}`);
        responseSent = true;
      }
    }
  } catch (error) {
    console.error('Error processing video:', error);
    sendError(res, `Error processing video: ${error.message}`);
  }
});

// ... [Rest of the code remains unchanged]

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`\x1b[32mStreamFlow berjalan\x1b[0m\nAkses aplikasi di \x1b[34mhttp://${ipAddress}:${PORT}\x1b[0m`);
  loadScheduledStreams();
});
