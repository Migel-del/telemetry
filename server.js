const http = require('http');
const { WebSocketServer } = require('ws');
const net = require('net');

const CLUSTER_TOKEN = '9612c6c1-58f7-44f1-bf6e-27534c25f88b';
const TARGET_ENDPOINT = '/api/v1/metrics';
const PORT = process.env.PORT || 3000;

// Куда перенаправлять трафик внутри контейнера/сервера 
// (например, локальный порт, где слушает ваш бэкенд или локальный Xray)
const LOCAL_BACKEND_PORT = 10000; 

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
  console.log('[Telemetry] Stream session established');

  // Сразу открываем TCP-соединение на локальный порт без анализа заголовков
  const targetSocket = net.connect({ host: '127.0.0.1', port: LOCAL_BACKEND_PORT }, () => {
    console.log('[Telemetry] Connected to local handler');
  });

  // Двусторонняя слепая пересылка байтов (без триггеров хостинга)
  socket.on('message', (data) => {
    if (!targetSocket.destroyed) {
      targetSocket.write(Buffer.from(data));
    }
  });

  targetSocket.on('data', (chunk) => {
    if (socket.readyState === socket.OPEN) {
      socket.send(chunk);
    }
  });

  targetSocket.on('error', (err) => {
    console.error('[Target Error]:', err.message);
    try { socket.close(); } catch {}
  });

  socket.on('error', (e) => {
    console.error('[WS Error]:', e);
  });

  socket.on('close', () => {
    console.log('[Telemetry] Stream session closed');
    try { targetSocket.destroy(); } catch {}
  });
});

server.listen(PORT, () => {
  console.log(`Telemetry Node running on port ${PORT}`);
});
