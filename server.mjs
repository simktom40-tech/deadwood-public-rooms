import http from 'node:http';
import { randomBytes } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';

export function createRelay({ roomLimit = 32, connectionLimit = 128 } = {}) {
  const rooms = new Map();
  const attempts = new Map();
  const version = 6;
  const clean = (s, limit) => typeof s === 'string' ? s.replace(/[^\p{L}\p{N} _-]/gu, '').trim().slice(0, limit) : '';
  const sendJSON = (ws, data) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); };
  const fail = (ws, message) => { sendJSON(ws, { type: 'error', message }); ws.close(1008, 'Request rejected'); };
  function relay(target, data, binary) {
    if (!target || target.readyState !== WebSocket.OPEN) return;
    if (target.bufferedAmount > 524288) { target.terminate(); return; }
    target.send(data, { binary });
  }
  const server = http.createServer((req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'");
    let path;
    try { path = new URL(req.url, 'http://relay.invalid').pathname; } catch { res.writeHead(400).end(); return; }
    if (req.method !== 'GET') { res.writeHead(405).end(); return; }
    if (path === '/healthz') { res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' }).end(JSON.stringify({ ok: true, protocol: version, rooms: rooms.size })); return; }
    if (path === '/api/rooms') {
      const list = [...rooms.values()].filter(r => r.host.readyState === WebSocket.OPEN).map(r => ({ id: r.id, name: r.name, host: r.alias, players: 1 + r.guests.size, capacity: 4, version }));
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' }).end(JSON.stringify({ protocol: version, rooms: list })); return;
    }
    if (path === '/') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }).end('<!doctype html><html lang="ru"><meta charset="utf-8"><title>DEADWOOD — комнаты</title><style>body{background:#102119;color:#eaf0dc;font:20px/1.6 system-ui;max-width:820px;margin:90px auto;padding:24px}h1{color:#d6e78c}</style><h1>DEADWOOD / PUBLIC ROOMS</h1><p>Служба комнат работает.</p><p>Открой игру → Онлайн → Создать комнату или выбери комнату в списке.</p><p>Входящие порты на компьютере игрока открывать не нужно.</p></html>'); return;
    }
    res.writeHead(404).end();
  });
  const wss = new WebSocketServer({ noServer: true, maxPayload: 65536, perMessageDeflate: false, maxFragments: 64 });
  server.on('upgrade', (req, socket, head) => {
    if (req.url !== '/relay' || wss.clients.size >= connectionLimit) { socket.end('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n'); return; }
    const ip = req.socket.remoteAddress || '?';
    const now = Date.now(); let slot = attempts.get(ip);
    if (!slot || slot.until < now) { slot = { count: 0, until: now + 60000 }; attempts.set(ip, slot); }
    if (++slot.count > 60) { socket.end('HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\n\r\n'); return; }
    wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws));
  });
  function detach(ws) {
    if (!ws.room) return;
    const room = ws.room; ws.room = null;
    if (ws.role === 'host') {
      rooms.delete(room.id);
      for (const guest of room.guests.values()) { guest.room = null; sendJSON(guest, { type: 'closed', message: 'Хост вышел из комнаты' }); guest.close(1000, 'Host left'); }
      room.guests.clear();
    } else if (room.guests.get(ws.slot) === ws) {
      room.guests.delete(ws.slot); sendJSON(room.host, { type: 'left', id: ws.slot });
    }
  }
  wss.on('connection', ws => {
    ws.alive = true; ws.room = null; ws.role = null; ws.byteCount = 0; ws.windowStart = Date.now();
    const helloTimeout = setTimeout(() => { if (!ws.room) ws.terminate(); }, 7000);
    ws.on('pong', () => { ws.alive = true; });
    ws.on('error', () => {});
    ws.on('close', () => { clearTimeout(helloTimeout); detach(ws); });
    ws.on('message', (data, binary) => {
      try {
        const now = Date.now(); if (now - ws.windowStart >= 1000) { ws.windowStart = now; ws.byteCount = 0; }
        ws.byteCount += data.length; if (ws.byteCount > 2097152) { ws.terminate(); return; }
        if (binary) {
          if (!ws.room || data.length < 1 || data.length > 32769) { fail(ws, 'Некорректный игровой пакет'); return; }
          if (ws.role === 'host') {
            const id = data[0]; if (id < 1 || id > 3) { fail(ws, 'Некорректный участник'); return; }
            relay(ws.room.guests.get(id), data.subarray(1), true);
          } else {
            const message = Buffer.allocUnsafe(data.length + 1); message[0] = ws.slot; data.copy(message, 1); relay(ws.room.host, message, true);
          }
          return;
        }
        if (data.length > 4096) { fail(ws, 'Запрос слишком большой'); return; }
        const msg = JSON.parse(data.toString('utf8'));
        if (!ws.room) {
          if (msg.version !== version) { fail(ws, 'Обнови игру до версии публичных комнат'); return; }
          if (msg.type === 'create') {
            if (rooms.size >= roomLimit) { fail(ws, 'Все комнаты сервера заняты. Попробуй позже'); return; }
            const name = clean(msg.name, 40), alias = clean(msg.alias, 16);
            if (!name || !alias || typeof msg.key !== 'string' || !/^[A-F0-9]{32}$/i.test(msg.key)) { fail(ws, 'Укажи имя и название комнаты'); return; }
            const id = randomBytes(12).toString('hex');
            const room = { id, name, alias, key: msg.key, host: ws, guests: new Map() };
            ws.room = room; ws.role = 'host'; rooms.set(id, room); clearTimeout(helloTimeout);
            sendJSON(ws, { type: 'created', id, name });
          } else if (msg.type === 'join') {
            const room = rooms.get(msg.id);
            if (!room || room.host.readyState !== WebSocket.OPEN) { fail(ws, 'Комната уже закрыта. Обнови список'); return; }
            let id = 1; while (room.guests.has(id) && id <= 3) id++;
            if (id > 3) { fail(ws, 'В комнате уже четыре игрока'); return; }
            ws.room = room; ws.role = 'guest'; ws.slot = id; room.guests.set(id, ws); clearTimeout(helloTimeout);
            sendJSON(ws, { type: 'joined', id: room.id, name: room.name, slot: id, key: room.key });
            sendJSON(room.host, { type: 'peer', id });
          } else fail(ws, 'Неизвестное действие');
        } else if (ws.role === 'host' && msg.type === 'kick' && Number.isInteger(msg.id)) {
          const guest = ws.room.guests.get(msg.id); if (guest) guest.close(1000, 'Host closed stream');
        } else if (msg.type !== 'keepalive') fail(ws, 'Неизвестное действие');
      } catch { fail(ws, 'Некорректный запрос'); }
    });
  });
  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); }
    const now = Date.now(); for (const [ip, item] of attempts) if (item.until < now) attempts.delete(ip);
  }, 20000);
  heartbeat.unref();
  return { server, rooms, wss,
    async listen(port = Number(process.env.PORT || 8787), host = '0.0.0.0') { await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, host, resolve); }); return server.address().port; },
    async close() { clearInterval(heartbeat); for (const ws of wss.clients) ws.terminate(); await new Promise(resolve => server.close(resolve)); wss.close(); }
  };
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const relay = createRelay(); const port = await relay.listen(); console.log(JSON.stringify({ status: 'ready', port }));
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, async () => { await relay.close(); process.exit(0); });
}
