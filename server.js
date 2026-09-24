const http = require('http');
const { WebSocketServer } = require('ws');
const net = require('net');

// Токен авторизации сессии телеметрии
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
        // Проверяем наличие и валидность токена в начале потока
        if (buffer.length < 16) {
          socket.close(1008, 'Invalid payload');
          return;
        }

        const tokenChunk = buffer.subarray(0, 16).toString('hex');
        const expectedToken = CLUSTER_TOKEN.replace(/-/g, '').toLowerCase();

        if (tokenChunk !== expectedToken) {
          console.log('[Collector] Unauthorized stream attempt');
          socket.close(1008, 'Authorization failed');
          return;
        }

        isAuthorized = true;
        console.log('[Telemetry] Stream session authorized successfully');

        // Отправляем подтверждение инициализации канала
        if (socket.readyState === socket.OPEN) {
          socket.send(Buffer.from([1]));
        }
        return;
      }

      // Если сессия авторизована, перенаправляем поток данных дальше
      if (targetSocket && !targetSocket.destroyed) {
        targetSocket.write(buffer);
      } else {
        // Динамический бэкенд-форвард при необходимости
        targetSocket = net.connect({ host: '127.0.0.1', port: 80 }, () => {
          targetSocket.write(buffer);
        });

        targetSocket.on('data', (chunk) => {
          if (socket.readyState === socket.OPEN) {
            socket.send(chunk);
          }
        });

        targetSocket.on('error', () => {
          try { socket.close(); } catch {}
        });

        targetSocket.on('close', () => {
          try { socket.close(); } catch {}
        });
      }

    } else (err) => {
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
