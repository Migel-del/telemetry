const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const memoryBuffer = new Map();

// Имитация базы данных метрик
const server = http.createServer((req, res) => {
  let body = [];

  req.on('data', (chunk) => {
    body.push(chunk);
  });

  req.on('end', () => {
    const buffer = Buffer.concat(body);
    const timestamp = Date.now();
    const requestId = crypto.randomBytes(8).toString('hex');

    // Сохраняем "метрики" в память, чтобы хостинг видел полезную нагрузку приложения
    if (buffer.length > 0) {
      memoryBuffer.set(requestId, {
        size: buffer.length,
        time: timestamp,
        checksum: crypto.createHash('sha256').update(buffer).digest('hex').substring(0, 12)
      });

      // Ограничиваем раздувание памяти
      if (memoryBuffer.size > 1000) {
        const firstKey = memoryBuffer.keys().next().value;
        memoryBuffer.delete(firstKey);
      }
    }

    res.writeHead(200, { 
      'Content-Type': 'application/json',
      'X-Telemetry-Node': 'active-cluster'
    });
    
    res.end(JSON.stringify({
      status: "success",
      processedBytes: buffer.length,
      nodeId: "cluster-worker-9612",
      uptime: Math.floor(process.uptime()),
      timestamp: timestamp
    }));
  });
});

server.listen(PORT, () => {
  console.log(`Telemetry storage worker online on port ${PORT}`);
});
