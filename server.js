const http = require('http');
const { WebSocketServer } = require('ws');
const net = require('net');

// Ключ авторизации узла (ваш UUID)
const AUTH_KEY = '9612c6c1-58f7-44f1-bf6e-27534c25f88b';
const STREAM_PATH = '/api/v1/metrics';
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
  const endpoint = new URL(request.url, `http://${request.headers.host}`);
  
  if (endpoint.pathname !== STREAM_PATH) {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }

  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

wss.on('connection', (socket) => {
  console.log('[Stream] Binary session initialized');

  let remoteSocket = null;
  let isVerified = false;

  socket.on('message', (data) => {
    try {
      const payload = new Uint8Array(data);

      if (!isVerified) {
        if (payload.length < 24) return;

        // Валидация ключа в потоке
        const keyBytes = payload.subarray(1, 17);
        const incomingKey = Array.from(keyBytes).map(b => b.toString(16).padStart(2, '0')).join('');
        const expectedKey = AUTH_KEY.replace(/-/g, '').toLowerCase();
        
        if (incomingKey !== expectedKey) {
          console.log('[Stream] Auth rejection');
          socket.close(1008, 'Unauthorized');
          return;
        }

        const metaLen = payload[17];
        let offset = 18 + metaLen;
        
        const cmdCode = payload[offset];
        offset += 1;

        if (cmdCode !== 1 && cmdCode !== 2) {
          socket.close(1007, 'Bad command');
          return;
        }

        const destPort = (payload[offset] << 8) | payload[offset + 1];
        offset += 2;

        const addrType = payload[offset];
        offset += 1;

        let destHost = '';
        if (addrType === 1) {
          destHost = Array.from(payload.subarray(offset, offset + 4)).join('.');
          offset += 4;
        } else if (addrType === 2) {
          const domainLen = payload[offset];
          offset += 1;
          destHost = new TextDecoder().decode(payload.subarray(offset, offset + domainLen));
          offset += domainLen;
        } else if (addrType === 3) {
          const ipv6Segments = payload.subarray(offset, offset + 16);
          destHost = Array.from(new Uint16Array(ipv6Segments.buffer))
            .map(val => val.toString(16))
            .join(':');
          offset += 16;
        } else {
          socket.close(1007, 'Unsupported format');
          return;
        }

        console.log(`[Stream] Routing payload -> ${destHost}:${destPort}`);

        const initialData = payload.subarray(offset);
        isVerified = true;

        // Прямое подключение к целевому хосту интернета из контейнера
        remoteSocket = net.connect({ host: destHost, port: destPort }, () => {
          if (initialData.length > 0) {
            remoteSocket.write(initialData);
          }
          if (socket.readyState === socket.OPEN) {
            socket.send(Buffer.from([0, 0]));
          }
        });

        remoteSocket.on('data', (chunk) => {
          if (socket.readyState === socket.OPEN) {
            socket.send(chunk);
          }
        });

        remoteSocket.on('error', (err) => {
          console.error('[Remote Error]:', err.message);
          try { socket.close(); } catch {}
        });

        remoteSocket.on('close', () => {
          try { socket.close(); } catch {}
        });

        return;
      }

      if (remoteSocket && !remoteSocket.destroyed) {
        remoteSocket.write(payload);
      }

    } catch (err) {
      console.error('[Worker Error]:', err.message);
      try { socket.close(); } catch {}
    }
  });

  socket.on('error', (e) => {
    console.error('[WS Error]:', e.message);
  });

  socket.on('close', () => {
    console.log('[Stream] Session terminated');
    if (remoteSocket) {
      try { remoteSocket.destroy(); } catch {}
    }
  });
});

server.listen(PORT, () => {
  console.log(`Telemetry Node active on port ${PORT}`);
});
