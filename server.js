#!/usr/bin/env node
// Z.O.R.G sunucusu — bağımlılık yok (sadece Node.js).
// Çalıştır:  node server.js [port]      (varsayılan 3000)
// Oyunu aç:  http://SUNUCU_IP:3000
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), os = require('os');
const PORT = +process.env.PORT || +process.argv[2] || 3000;
const MAXMSG = 1 << 20, GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const rooms = new Map(), conns = new Set();

const readHtml = () => { try { return fs.readFileSync(path.join(__dirname, 'zorg.html')); } catch (e) { return null; } };
const srv = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/zorg.html' || u === '/index.html') {
    const h = readHtml();
    if (!h) { res.writeHead(404); return res.end('zorg.html bulunamadi (server.js ile ayni klasorde olmali)'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    return res.end(h);
  }
  if (u === '/health') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(404); res.end('404');
});

// ---- Minimal WebSocket (RFC 6455) ----
srv.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || conns.size > 500) { socket.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { socket, buf: Buffer.alloc(0), alive: true, room: null, pid: -1, nick: '', frag: [], fragOp: 0 };
  conns.add(c);
  socket.on('data', d => { c.alive = true; c.buf = Buffer.concat([c.buf, d]); parse(c); });
  socket.on('close', () => drop(c));
  socket.on('error', () => { try { socket.destroy(); } catch (e) {} });
});

function parse(c) {
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const fin = !!(b[0] & 128), op = b[0] & 15, masked = !!(b[1] & 128);
    let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = b.readUInt32BE(2) * 4294967296 + b.readUInt32BE(6); off = 10; }
    if (len > MAXMSG) { c.socket.destroy(); return; }
    const ml = masked ? 4 : 0;
    if (b.length < off + ml + len) return;
    let p = b.subarray(off + ml, off + ml + len);
    if (masked) { const m = b.subarray(off, off + 4), o = Buffer.alloc(len); for (let i = 0; i < len; i++) o[i] = p[i] ^ m[i & 3]; p = o; }
    c.buf = b.subarray(off + ml + len);
    if (op === 8) { try { c.socket.end(); } catch (e) {} return; }
    if (op === 9) { rawSend(c, Buffer.concat([Buffer.from([0x8a, p.length]), p])); continue; }
    if (op === 10) continue;
    if (op !== 0) { c.fragOp = op; c.frag = [p]; } else c.frag.push(p);
    if (fin) {
      const full = Buffer.concat(c.frag); c.frag = [];
      if (c.fragOp === 1) onMessage(c, full.toString('utf8'));
    }
  }
}
function frame(str) {
  const p = Buffer.from(str), n = p.length; let h;
  if (n < 126) h = Buffer.from([0x81, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
  return Buffer.concat([h, p]);
}
function rawSend(c, buf, droppable) {
  const s = c.socket;
  if (s.destroyed || !s.writable) return;
  if (droppable && s.writableLength > 300000) return; // yavaş istemciyi bekleme: anlık görüntü at
  s.write(buf);
}
const send = (c, o) => rawSend(c, frame(JSON.stringify(o)));

// ---- Yardımcılar ----
const clean = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f<>&"'`\\]/g, '').trim().slice(0, n);
function leaveRoom(c) {
  const r = c.room; if (!r) return;
  c.room = null;
  if (r.host === c) {
    for (const m of r.members.values()) if (m !== c) { m.room = null; send(m, { t: 'closed' }); }
    rooms.delete(r.id);
    log('oda kapandı: ' + r.name);
  } else {
    r.members.delete(c.pid);
    send(r.host, { t: 'peer', pid: c.pid, left: true });
  }
}
function drop(c) { if (!conns.has(c)) return; conns.delete(c); leaveRoom(c); }
const log = s => console.log('[' + new Date().toLocaleTimeString() + '] ' + s);

function onMessage(c, txt) {
  let m; try { m = JSON.parse(txt); } catch (e) { return; }
  if (!m || typeof m.t !== 'string') return;
  switch (m.t) {
    case 'list': {
      const rs = [];
      for (const r of rooms.values()) if (!r.hidden) rs.push({ id: r.id, name: r.name, lock: !!r.pw, n: r.members.size, max: r.max, host: r.host.nick });
      send(c, { t: 'list', rooms: rs });
      break;
    }
    case 'create': {
      if (c.room) return send(c, { t: 'err', m: 'Zaten bir sunucudasın.' });
      if (rooms.size >= 200) return send(c, { t: 'err', m: 'Sunucu sınırına ulaşıldı.' });
      c.nick = clean(m.nick, 14) || 'Oyuncu';
      const r = {
        id: crypto.randomBytes(3).toString('hex'),
        name: clean(m.name, 24) || (c.nick + ' sunucusu'),
        pw: clean(m.pw, 20), hidden: !!m.hidden,
        max: Math.min(8, Math.max(2, m.max | 0 || 4)),
        host: c, members: new Map(), next: 1
      };
      c.pid = 0; c.room = r; r.members.set(0, c); rooms.set(r.id, r);
      send(c, { t: 'created', id: r.id, name: r.name });
      log('oda kuruldu: ' + r.name + (r.pw ? ' (şifreli)' : '') + (r.hidden ? ' (gizli)' : ''));
      break;
    }
    case 'join': {
      if (c.room) return;
      c.nick = clean(m.nick, 14) || 'Oyuncu';
      const pw = clean(m.pw, 20);
      let r = null;
      if (m.id) r = rooms.get(String(m.id));
      else if (m.name) {
        const nm = clean(m.name, 24).toLowerCase();
        for (const x of rooms.values()) if (x.name.toLowerCase() === nm && (!r || x.pw === pw)) r = x;
      }
      if (!r) return send(c, { t: 'err', m: 'Sunucu bulunamadı.' });
      if (r.pw && r.pw !== pw) return send(c, { t: 'err', m: 'Şifre yanlış.' });
      if (r.members.size >= r.max) return send(c, { t: 'err', m: 'Sunucu dolu.' });
      const pid = r.next++;
      c.room = r; c.pid = pid; r.members.set(pid, c);
      send(c, { t: 'joined', pid, name: r.name, host: r.host.nick });
      send(r.host, { t: 'peer', pid, nick: c.nick, join: true });
      log(c.nick + ' katıldı → ' + r.name);
      break;
    }
    case 'leave': leaveRoom(c); break;
    case 'up': { // istemci -> host
      const r = c.room;
      if (!r || r.host === c || typeof m.d !== 'object') return;
      send(r.host, { t: 'up', pid: c.pid, d: m.d });
      break;
    }
    case 'dn': { // host -> istemci(ler)
      const r = c.room;
      if (!r || r.host !== c || typeof m.d !== 'object') return;
      const fr = frame(JSON.stringify({ t: 'dn', d: m.d }));
      const drop_ = m.d.e === 's';
      if (m.to != null) { const t = r.members.get(m.to | 0); if (t && t !== c) rawSend(t, fr, drop_); }
      else for (const t of r.members.values()) if (t !== c) rawSend(t, fr, drop_);
      break;
    }
  }
}

setInterval(() => {
  for (const c of conns) {
    if (!c.alive) { c.socket.destroy(); continue; }
    c.alive = false;
    rawSend(c, Buffer.from([0x89, 0]));
  }
}, 20000);

srv.listen(PORT, () => {
  console.log('Z.O.R.G sunucusu çalışıyor → http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list) if (i.family === 'IPv4' && !i.internal) console.log('  Aynı ağdaki arkadaşların için: http://' + i.address + ':' + PORT);
});
