const http = require('http');
const { WebSocketServer } = require('ws');
const net = require('net');

// Маскируем UUID под токен конфигурации узла телеметрии
const CLUSTER_TOKEN = '9612c6c1-58f7-44f1-bf6e-27534c25f88b';
const TARGET_ENDPOINT = '/api/v1/metrics';
const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ 
    service: "telemetry-collector-node",
    status: "active", 
    uptime: process.uptime(),
    timestamp: Date.now() 
  }));
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  
  // Если путь не совпадает — отдаем 404 под видом обычного API
  if (url.pathname !== TARGET_ENDPOINT) {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }

  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

wss.on('connection', (socket) => {
  console.log('[Collector] Telemetry stream session established');

  let targetSocket = null;
  let isAuthorized = false;

  socket.on('message', (data) => {
    try {
      const buffer = Buffer.from(data);

      if (!isAuthorized) {
        if (buffer.length < 16) {
          console.log('[Collector] Payload fragment too small:', buffer.length);
          socket.close(1008, 'Authorization failed');
          return;
        }

        // Проверка токена узла телеметрии
        const tokenChunk = buffer.subarray(0, 16).toString('hex');
        const expectedToken = CLUSTER_TOKEN.replace(/-/g, '').toLowerCase();
        
        if (tokenChunk !== expectedToken) {
          console.log('[Collector] Unauthorized token attempt');
          socket.close(1008, 'Authorization failed');
          return;
        }

        isAuthorized = true;
        console.log('[Telemetry] Stream session authorized successfully');

        // Отправляем подтверждение инициализации канала
        if (socket.readyState === socket.OPEN) {
          socket.send(Buffer.from([0, 0]));
        }
        return;
      }

      // Пересылка потока данных, если сокет инициализирован
      if (targetSocket && !targetSocket.destroyed) {
        targetSocket.write(buffer);
      } else {
        // Резервный локальный форвард при отсутствии внешнего дескриптора
        targetSocket = net.connect({ host: '127.0.0.1', port: 80 }, () => {
          targetSocket.write(buffer);
        });

        targetSocket.on('data', (chunk) => {
          if (socket.readyState === socket.OPEN) {
            socket.send(chunk);
          }
        });

        targetSocket.on('error', (err) => {
          console.error('[Collector Error]:', err.message);
          try { socket.close(); } catch {}
        });

        targetSocket.on('close', () => {
          try { socket.close(); } catch {}
        });
      }

    } catch (err) {
      console.error('[Worker Exception]:', err);
      try { socket.close(); } catch {}
    }
  });

  socket.on('error', (e) => {
    console.error('[WS Stream Error]:', e);
  });

  socket.on('close', () => {
    console.log('[Collector] Stream session closed');
    if (targetSocket) {
      try { targetSocket.destroy(); } catch {}
    }
  });
});

server.listen(PORT, () => {
  console.log(`Telemetry Node running on port ${PORT}`);
});
