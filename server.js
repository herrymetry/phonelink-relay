const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('PhoneLink relay is running.\n');
});

const wss = new WebSocketServer({ server, path: '/ws' });

const rooms = new Map();

function getRoom(code) {
  let room = rooms.get(code);
  if (!room) {
    room = { hostWs: null, sessions: new Map() };
    rooms.set(code, room);
  }
  return room;
}

function cleanupRoomIfEmpty(code, room) {
  if (!room.hostWs && room.sessions.size === 0) {
    rooms.delete(code);
  }
}

function genId() {
  return Math.random().toString(36).slice(2, 10);
}

wss.on('connection', (ws, req) => {
  let url;
  try {
    url = new URL(req.url, 'http://localhost');
  } catch (e) {
    ws.close();
    return;
  }
  const role = url.searchParams.get('role');
  const code = (url.searchParams.get('code') || '').toUpperCase();

  if (!code || !role) {
    ws.close();
    return;
  }

  const room = getRoom(code);

  if (role === 'host') {
    room.hostWs = ws;
    ws.on('close', () => {
      if (room.hostWs === ws) room.hostWs = null;
      cleanupRoomIfEmpty(code, room);
    });
    ws.on('error', () => {});
    return;
  }

  if (role === 'client') {
    if (!room.hostWs || room.hostWs.readyState !== WebSocket.OPEN) {
      ws.close(4000, 'host not available');
      return;
    }
    const sessionId = genId();
    const sess = { clientWs: ws, hostDataWs: null, buffer: [] };
    room.sessions.set(sessionId, sess);

    room.hostWs.send(JSON.stringify({ type: 'new_session', sessionId }));

    ws.on('message', (data) => {
      if (sess.hostDataWs && sess.hostDataWs.readyState === WebSocket.OPEN) {
        sess.hostDataWs.send(data);
      } else {
        sess.buffer.push(data);
      }
    });
    ws.on('close', () => {
      if (sess.hostDataWs) sess.hostDataWs.close();
      room.sessions.delete(sessionId);
      cleanupRoomIfEmpty(code, room);
    });
    ws.on('error', () => {});
    return;
  }

  if (role === 'host_data') {
    const sessionId = url.searchParams.get('sessionId');
    const sess = sessionId ? room.sessions.get(sessionId) : null;
    if (!sess) {
      ws.close(4001, 'unknown session');
      return;
    }
    sess.hostDataWs = ws;
    for (const buffered of sess.buffer) ws.send(buffered);
    sess.buffer = [];

    ws.on('message', (data) => {
      if (sess.clientWs && sess.clientWs.readyState === WebSocket.OPEN) {
        sess.clientWs.send(data);
      }
    });
    ws.on('close', () => {
      if (sess.clientWs) sess.clientWs.close();
      room.sessions.delete(sessionId);
      cleanupRoomIfEmpty(code, room);
    });
    ws.on('error', () => {});
    return;
  }

  ws.close();
});

server.listen(PORT, () => {
  console.log(`PhoneLink relay listening on port ${PORT}`);
});
