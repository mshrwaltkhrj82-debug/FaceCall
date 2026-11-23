const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');
const admin = require('firebase-admin');

const PORT = process.env.PORT || 8080;

// ==================== 1. تهيئة Firebase Firestore ====================
function initFirebase() {
  let serviceAccount;

  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } else {
    // للاستخدام المحلي على جهازك فقط (لا ترفع ملف JSON على GitHub)
    const localKeyPath = path.join(__dirname, 'facecall-d336d-firebase-adminsdk-fbsvc-9b462096d1.json');
    if (!fs.existsSync(localKeyPath)) {
      throw new Error('Missing FIREBASE_SERVICE_ACCOUNT environment variable.');
    }
    serviceAccount = JSON.parse(fs.readFileSync(localKeyPath, 'utf8'));
  }

  // معالجة أسطر المفتاح الخاص عند نسخه في Railway Variables
  if (serviceAccount.private_key) {
    serviceAccount.private_key = serviceAccount.private_key.replace(/\\n/g, '\n');
  }

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });

  return admin.firestore();
}

const firestore = initFirebase();
const usersCol = firestore.collection('users');

// ==================== 2. خادم HTTP + WebSocket ====================
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('SilentVideo Global Server (Firestore Connected) is Running.');
});

const wss = new WebSocket.Server({ server });

const onlineSockets = new Map();      // username -> ws
const onlineUserContacts = new Map(); // username -> Set(contactUsernames)
const pendingOtps = new Map();        // phone -> code

// جلب خوادم STUN + TURN العالمية
async function getGlobalIceServers() {
  const iceServers = [
    { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'], username: '', credential: '' }
  ];

  if (process.env.METERED_DOMAIN && process.env.METERED_API_KEY) {
    try {
      const url = `https://${process.env.METERED_DOMAIN}/api/v1/turn/credentials?apiKey=${process.env.METERED_API_KEY}`;
      const response = await fetch(url);
      if (response.ok) {
        const meteredServers = await response.json();
        for (const item of meteredServers) {
          iceServers.push({
            urls: Array.isArray(item.urls) ? item.urls : [item.urls],
            username: item.username || '',
            credential: item.credential || ''
          });
        }
        return iceServers;
      }
    } catch (err) {
      console.error('Metered TURN fetch failed:', err.message);
    }
  }

  if (process.env.TURN_URLS) {
    iceServers.push({
      urls: process.env.TURN_URLS.split(',').map(u => u.trim()).filter(Boolean),
      username: process.env.TURN_USERNAME || '',
      credential: process.env.TURN_CREDENTIAL || ''
    });
  }

  return iceServers;
}

function formatNow() {
  const d = new Date();
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const mo = String(d.getMonth() + 1).padStart(2, '0');
  return `${dd}/${mo} ${hh}:${mm}`;
}

// ==================== 3. دوال Firestore ====================
async function getUserDoc(username) {
  const snap = await usersCol.doc(username).get();
  return snap.exists ? snap.data() : null;
}

async function findUserByIdentifier(query) {
  const raw = (query || '').trim();
  const cleanUser = raw.toLowerCase().replace(/^@/, '');
  if (!cleanUser) return null;

  const byUser = await getUserDoc(cleanUser);
  if (byUser) return byUser;

  const byPhoneSnap = await usersCol.where('phone', '==', raw).limit(1).get();
  if (!byPhoneSnap.empty) return byPhoneSnap.docs[0].data();

  return null;
}

async function buildContactsPayload(contactUsernames) {
  const list = Array.isArray(contactUsernames) ? contactUsernames : [];
  if (list.length === 0) return [];

  const refs = list.map(u => usersCol.doc(u));
  const snaps = await firestore.getAll(...refs);

  return snaps
    .filter(s => s.exists)
    .map(s => {
      const data = s.data();
      return {
        username: data.username,
        phone: data.phone || '',
        online: onlineSockets.has(data.username)
      };
    });
}

async function addCallLog(username, peer, direction, status) {
  const ref = usersCol.doc(username);
  const snap = await ref.get();
  if (!snap.exists) return;

  const data = snap.data();
  const logs = Array.isArray(data.callLogs) ? data.callLogs : [];
  logs.unshift({ peer, direction, status, time: formatNow() });
  const trimmed = logs.slice(0, 40);

  await ref.update({ callLogs: trimmed });
}

async function ensureContact(ownerUsername, targetUsername) {
  const ref = usersCol.doc(ownerUsername);
  const snap = await ref.get();
  if (!snap.exists) return;

  const data = snap.data();
  const contacts = Array.isArray(data.contacts) ? data.contacts : [];
  if (!contacts.includes(targetUsername)) {
    contacts.unshift(targetUsername);
    await ref.update({ contacts });
    if (onlineUserContacts.has(ownerUsername)) {
      onlineUserContacts.get(ownerUsername).add(targetUsername);
    }
  }
}

async function sendSyncState(ws, username) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  const userData = await getUserDoc(username);
  if (!userData) return;

  const contactsList = Array.isArray(userData.contacts) ? userData.contacts : [];
  onlineUserContacts.set(username, new Set(contactsList));

  ws.send(JSON.stringify({
    type: 'sync_state',
    contacts: await buildContactsPayload(contactsList),
    callLogs: Array.isArray(userData.callLogs) ? userData.callLogs : []
  }));
}

async function broadcastPresence(changedUser) {
  const tasks = [];
  for (const [user, contactSet] of onlineUserContacts.entries()) {
    if (contactSet.has(changedUser)) {
      const socket = onlineSockets.get(user);
      if (socket && socket.readyState === WebSocket.OPEN) {
        tasks.push(sendSyncState(socket, user));
      }
    }
  }
  await Promise.allSettled(tasks);
}

// ==================== 4. معالجة اتصالات WebSocket ====================
wss.on('connection', (ws) => {
  let currentUsername = null;
  ws.isAlive = true;

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    try {
      if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
        return;
      }

      // 1) طلب OTP
      if (msg.type === 'request_otp') {
        const phone = (msg.phone || '').trim();
        const code = Math.floor(100000 + Math.random() * 900000).toString();
        pendingOtps.set(phone, code);
        console.log(`[OTP] ${phone} -> ${code}`);
        ws.send(JSON.stringify({ type: 'otp_sent', message: code }));
        return;
      }

      // 2) تسجيل حساب جديد في Firestore
      if (msg.type === 'register') {
        const username = (msg.username || '').trim().toLowerCase().replace(/^@/, '');
        const phone = (msg.phone || '').trim();

        if (username.length < 3) {
          ws.send(JSON.stringify({ type: 'error', message: 'Username must be at least 3 chars.' }));
          return;
        }

        if (phone) {
          const expected = pendingOtps.get(phone);
          if (!expected || expected !== (msg.otp || '').trim()) {
            ws.send(JSON.stringify({ type: 'error', message: 'Invalid OTP code.' }));
            return;
          }
          pendingOtps.delete(phone);

          const phoneSnap = await usersCol.where('phone', '==', phone).limit(1).get();
          if (!phoneSnap.empty && phoneSnap.docs[0].id !== username) {
            ws.send(JSON.stringify({
              type: 'error',
              message: 'Phone already registered as @' + phoneSnap.docs[0].id
            }));
            return;
          }
        }

        const existingUser = await getUserDoc(username);
        if (existingUser) {
          ws.send(JSON.stringify({ type: 'error', message: 'Username already taken.' }));
          return;
        }

        const token = crypto.randomBytes(16).toString('hex');
        const newUser = {
          username,
          phone,
          token,
          contacts: [],
          callLogs: [],
          createdAt: Date.now()
        };

        await usersCol.doc(username).set(newUser);

        currentUsername = username;
        onlineSockets.set(currentUsername, ws);
        onlineUserContacts.set(currentUsername, new Set());

        const iceServers = await getGlobalIceServers();
        ws.send(JSON.stringify({
          type: 'auth_ok',
          username,
          phone,
          token,
          iceServers,
          contacts: [],
          callLogs: []
        }));
        await broadcastPresence(username);
        return;
      }

      // 3) تسجيل الدخول التلقائي الدائم عبر Token
      if (msg.type === 'token_login') {
        const username = (msg.username || '').trim().toLowerCase();
        const user = await getUserDoc(username);

        if (!user || user.token !== msg.token) {
          ws.send(JSON.stringify({ type: 'auth_expired' }));
          return;
        }

        currentUsername = username;
        onlineSockets.set(currentUsername, ws);
        const contactsList = Array.isArray(user.contacts) ? user.contacts : [];
        onlineUserContacts.set(currentUsername, new Set(contactsList));

        const iceServers = await getGlobalIceServers();
        ws.send(JSON.stringify({
          type: 'auth_ok',
          username: user.username,
          phone: user.phone || '',
          token: user.token,
          iceServers,
          contacts: await buildContactsPayload(contactsList),
          callLogs: Array.isArray(user.callLogs) ? user.callLogs : []
        }));
        await broadcastPresence(username);
        return;
      }

      if (!currentUsername) return;

      // 4) إضافة جهة اتصال برقم الهاتف أو اليوزر
      if (msg.type === 'add_contact') {
        const found = await findUserByIdentifier(msg.query);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: 'User/Phone not found.' }));
          return;
        }
        if (found.username === currentUsername) {
          ws.send(JSON.stringify({ type: 'error', message: 'Cannot add yourself.' }));
          return;
        }

        await ensureContact(currentUsername, found.username);
        await sendSyncState(ws, currentUsername);
        ws.send(JSON.stringify({ type: 'info', message: 'Added @' + found.username }));
        return;
      }

      // 5) طلب مكالمة فيديو صامتة
      if (msg.type === 'call_request') {
        const found = await findUserByIdentifier(msg.to);
        if (!found) {
          ws.send(JSON.stringify({ type: 'error', message: 'User not found.' }));
          return;
        }
        const targetUser = found.username;
        const targetWs = onlineSockets.get(targetUser);

        await ensureContact(currentUsername, targetUser);

        if (!targetWs || targetWs.readyState !== WebSocket.OPEN) {
          await Promise.all([
            addCallLog(currentUsername, targetUser, 'outgoing', 'missed'),
            addCallLog(targetUser, currentUsername, 'incoming', 'missed')
          ]);
          await sendSyncState(ws, currentUsername);
          ws.send(JSON.stringify({ type: 'call_unavailable', message: '@' + targetUser + ' is offline.' }));
          return;
        }

        targetWs.send(JSON.stringify({ type: 'incoming_call', from: currentUsername }));
        ws.send(JSON.stringify({ type: 'call_ringing', to: targetUser }));
        return;
      }

      if (msg.type === 'call_cancel') {
        const targetUser = msg.to;
        await Promise.all([
          addCallLog(currentUsername, targetUser, 'outgoing', 'missed'),
          addCallLog(targetUser, currentUsername, 'incoming', 'missed')
        ]);
        await sendSyncState(ws, currentUsername);

        const targetWs = onlineSockets.get(targetUser);
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(JSON.stringify({ type: 'call_cancelled', from: currentUsername }));
          await sendSyncState(targetWs, targetUser);
        }
        return;
      }

      if (msg.type === 'call_accept') {
        const caller = msg.to;
        await Promise.all([
          addCallLog(caller, currentUsername, 'outgoing', 'answered'),
          addCallLog(currentUsername, caller, 'incoming', 'answered')
        ]);
        await sendSyncState(ws, currentUsername);

        const callerWs = onlineSockets.get(caller);
        if (callerWs && callerWs.readyState === WebSocket.OPEN) {
          callerWs.send(JSON.stringify({ type: 'call_accepted', from: currentUsername }));
          await sendSyncState(callerWs, caller);
        }
        return;
      }

      if (msg.type === 'call_reject') {
        const caller = msg.to;
        await Promise.all([
          addCallLog(caller, currentUsername, 'outgoing', 'rejected'),
          addCallLog(currentUsername, caller, 'incoming', 'rejected')
        ]);
        await sendSyncState(ws, currentUsername);

        const callerWs = onlineSockets.get(caller);
        if (callerWs && callerWs.readyState === WebSocket.OPEN) {
          callerWs.send(JSON.stringify({ type: 'call_rejected', from: currentUsername }));
          await sendSyncState(callerWs, caller);
        }
        return;
      }

      // توجيه إشارات WebRTC وحالة الكاميرا وإنهاء المكالمة
      const targetWs = onlineSockets.get(msg.to);
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        const forwardType = msg.type === 'call_end' ? 'call_ended' : msg.type;
        targetWs.send(JSON.stringify({ ...msg, from: currentUsername, type: forwardType }));
      }
    } catch (err) {
      console.error('Message handling error:', err);
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message: 'Server error, please retry.' }));
      }
    }
  });

  ws.on('close', () => {
    if (currentUsername && onlineSockets.get(currentUsername) === ws) {
      onlineSockets.delete(currentUsername);
      onlineUserContacts.delete(currentUsername);
      broadcastPresence(currentUsername);
    }
  });
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`SilentVideo Firestore Server listening on port ${PORT}`);
});
