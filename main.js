process.env.UV_THREADPOOL_SIZE = 4096;
try {
  process.setpriority(process.pid, -20);
  require('child_process').execSync(`powershell "Get-Process -Id ${process.pid} | ForEach-Object { $_.PriorityClass = 'RealTime' }"`);
} catch {}



"use strict";

const tls = require("tls");
const WebSocket = require("ws");
const fs = require("fs");
const http2 = require("http2");
const token = "";
const server = "1476188095744577598";
const channel = "1570945805462798346";
const PATCH_URL = `/api/v9/guilds/${server}/vanity-url`;
const tlspatchi = `/api/v7/guilds/${server}/vanity-url`;
const http_session_sayisi = 1;   // 1 HTTP/2 session (1 PATCH)
const tls_pool_sayisi = 1;       // 5 TLS baglantisi
const PIPELINE_DEPTH = 1;        // Her baglanti 4 PATCH
const WS_CONNECTION_COUNT = 1;
const wsMeta = [
  { name: "main", url: "wss://gateway.discord.gg/?v=9&encoding=json" }
];
const tlsTarget = { host: "canary.discord.com", servername: "canary.discord.com" };
const STATIC_HEADERS = {
  'user-agent': 'Chrome/124',
  'x-super-properties': 'eyJicm93c2VyIjoiRmlyZWZveCIsImJyb3dzZXJfdXNlcl9hZ2VudCI6IkZpcmVmb3gifQ==',
  'content-type': 'application/json'
};
let mfaToken = "";
let vanity = "";
// { [guildId]: { vanity: string, tier: number } }
const guilds = {};
const tlsPool = new Array(tls_pool_sayisi);
const http2Sessions = new Array(http_session_sayisi).fill(null);
const websockets = [];
const sessionCache = new Map();
const requestCache = new Map();
let currentGatewayIndex = 0;
let sessionIndex = 0;
let http2Load = new Array(http_session_sayisi).fill(0);
let pipelineLoad = new Array(tls_pool_sayisi).fill(0);
const bufferPool = {
  small: [],
  medium: [],
  large: []
};
function getBuffer(size) {
  if (size <= 1024) return bufferPool.small.pop() || Buffer.allocUnsafe(1024);
  if (size <= 4096) return bufferPool.medium.pop() || Buffer.allocUnsafe(4096);
  return bufferPool.large.pop() || Buffer.allocUnsafe(16384);
}
function returnBuffer(buffer) {
  const size = buffer.length;
  if (size === 1024 && bufferPool.small.length < 100) bufferPool.small.push(buffer);
  else if (size === 4096 && bufferPool.medium.length < 100) bufferPool.medium.push(buffer);
  else if (size === 16384 && bufferPool.large.length < 100) bufferPool.large.push(buffer);
}
const parseJSON = (() => {
  const cache = new Map();
  return (str) => {
    if (!str || str === '{}') return null;
    if (cache.has(str)) return cache.get(str);
    try {
      const result = JSON.parse(str);
      if (cache.size < 1000) cache.set(str, result);
      return result;
    } catch {
      return null;
    }
  };
})();
function getVanityPatchRequestBufferTLS(vanityCode) {
  const payload = JSON.stringify({ code: vanityCode });
  const payloadLength = Buffer.byteLength(payload);
  return Buffer.from(
    `PATCH ${tlspatchi} HTTP/1.1\r\n` +
    `Host: canary.discord.com\r\n` +
    `Connection: keep-alive\r\n` +
    `Keep-Alive: timeout=300, max=1000\r\n` +
    `Authorization: ${token}\r\n` +
    `x-discord-mfa-authorization: ${mfaToken}\r\n` +
    `User-Agent: Chrome/124\r\n` +
    `x-super-properties: eyJicm93c2VyIjoiRmlyZWZveCIsImJyb3dzZXJfdXNlcl9hZ2VudCI6IkZpcmVmb3gifQ==\r\n` +
    `Content-Type: application/json\r\n` +
    `Content-Length: ${payloadLength}\r\n\r\n` +
    payload
  );
}
function getVanityPatchRequestBufferHTTP2(vanityCode) {
  return Buffer.from(JSON.stringify({ code: vanityCode }));
}
function getPipelinedRequestsTLS(vanityCode, count) {
  const single = getVanityPatchRequestBufferTLS(vanityCode);
  const pipelined = Buffer.allocUnsafe(single.length * count);
  for (let i = 0; i < count; i++) {
    single.copy(pipelined, i * single.length);
  }
  return pipelined;
}
function logToChannel(parsed, type) {
  try {
    const body = JSON.stringify({
      content: `@everyone ${vanity || ''} - ${type}\n\`\`\`json\n${JSON.stringify(parsed, null, 2)}\`\`\``
    });   
    const session = http2Sessions[sessionIndex % http2Sessions.length];
    if (session && !session.destroyed && !session.closed) {
      const req = session.request({
        ':method': 'POST',
        ':path': `/api/channels/${channel}/messages`,
        'authorization': token,
        'content-type': 'application/json'
      });
      req.on('error', () => {});
      req.write(body);
      req.end();
      http2Load[sessionIndex % http2Sessions.length]++;
    }
    sessionIndex = (sessionIndex + 1) % http2Sessions.length;
  } catch {}
}
function createHttp2Session(index) {
  const session = http2.connect('https://canary.discord.com', {
    settings: {
      headerTableSize: 65536,
      enablePush: false,
      initialWindowSize: 1048576,
      maxFrameSize: 16384,
      maxConcurrentStreams: 250,
      maxHeaderListSize: 65536,
      enableHeaderCompression: true
    }
  });
  session.on('connect', () => {
    if (!http2Sessions.includes(session)) {
      http2Sessions[index] = session;
    }
  });
  session.on('error', (err) => {
    console.error(`HTTP/2 session error at index ${index}:`, err.message);
    reconnectSession(index);
  });
  session.on('close', () => reconnectSession(index));
  return session;
}
function reconnectSession(index) {
  http2Sessions[index] = null;
  setImmediate(() => createHttp2Session(index));
}
function handleHttp2Response(req, sessionIdx) {
  let responseData = '';
  req.on('data', chunk => responseData += chunk);
  req.on('end', () => {
    try {
      const parsed = parseJSON(responseData || "{}");
      if (parsed && (parsed.code || parsed.message)) {
        logToChannel(parsed, 'HTTP2');
      }
    } catch {}
    http2Load[sessionIdx]--;
  });
  req.on('error', () => {
    http2Load[sessionIdx]--;
  });
}
const sendPatchRequestHTTP2 = (code) => {
  const body = getVanityPatchRequestBufferHTTP2(code);
  for (let i = 0; i < http_session_sayisi; i++) {
    const session = http2Sessions[i];
    if (session && !session.destroyed && !session.closed) {
      const req = session.request({
        ':method': 'PATCH',
        ':path': PATCH_URL,
        'authorization': token,
        'x-discord-mfa-authorization': mfaToken,
        'user-agent': STATIC_HEADERS['user-agent'],
        'x-super-properties': STATIC_HEADERS['x-super-properties'],
        'content-type': STATIC_HEADERS['content-type'],
        'weight': 256,
        'parent': 0,
        'exclusive': true
      });
      http2Load[i]++;
      handleHttp2Response(req, i);
      req.write(body);
      req.end();
    }
  }
};
const sendPatchRequestTLS = (vanityCode) => {
  const single = getVanityPatchRequestBufferTLS(vanityCode);
  for (let i = 0; i < tls_pool_sayisi; i++) {
    const conn = tlsPool[i];
    if (conn?.writable && conn._isPipelined && pipelineLoad[i] < 100) {
      conn.write(single);
      pipelineLoad[i]++;
    }
  }
};
// ==================== TLS CONNECTION MANAGEMENT ====================
function createTlsConnection(index) {
  const options = {
    ...tlsTarget,
    port: 443,
    minVersion: 'TLSv1.3',
    maxVersion: 'TLSv1.3',
    handshakeTimeout: 0,
    rejectUnauthorized: false,
    enableTrace: false,
    requestCert: false,
    highWaterMark: 1048576,
    noDelay: true,
    keepAlive: true,
    keepAliveInitialDelay: 0
  };
 
  if (sessionCache.has('canary.discord.com')) {
    options.session = sessionCache.get('canary.discord.com');
  }
 
  const conn = tls.connect(options);
  conn.setNoDelay(true);
  conn.setKeepAlive(true, 0);
  conn._buffer = "";
  conn._isPipelined = true;
 
  conn.on("data", data => {
    conn._buffer += data.toString();
    if (conn._buffer.includes("\r\n\r\n")) {
      const body = conn._buffer.split("\r\n\r\n")[1] || "";
      try {
        const match = body.match(/\{[\s\S]*?\}/);
        if (match) {
          const parsed = parseJSON(match[0]);
          if (parsed && (parsed.code || parsed.message)) {
            logToChannel(parsed, 'TLS');
          }
        }
      } catch {}
      conn._buffer = "";
      pipelineLoad[index]--;
    }
  });
  const cleanup = () => {
    if (tlsPool[index] === conn) {
      tlsPool[index] = null;
    }
    setTimeout(() => {
      tlsPool[index] = createTlsConnection(index);
    }, 50);
  };
  conn.on("error", cleanup);
  conn.on("close", cleanup);
  conn.on("secureConnect", () => {
    conn.write('GET / HTTP/1.1\r\nHost: canary.discord.com\r\nConnection: keep-alive\r\n\r\n');
  });
  conn.on("session", session => {
    sessionCache.set('canary.discord.com', session);
  });
 
  return conn;
}
function createWebSocketConnection(index) {
  const gateway = wsMeta[currentGatewayIndex];
  const ws = new WebSocket(gateway.url);
  ws.on('open', () => {
    ws._socket.setNoDelay(true);
    ws._socket.setKeepAlive(true, 0);
  });
  ws.onclose = () => {
    const idx = websockets.indexOf(ws);
    if (idx !== -1) websockets.splice(idx, 1);
    currentGatewayIndex = (currentGatewayIndex + 1) % wsMeta.length;
    setTimeout(() => {
      const newWs = createWebSocketConnection(index);
      websockets[index] = newWs;
    }, 5000);
  };
  ws.on('error', () => {});
  ws.onmessage = handleWebSocketMessage;
  return ws;
}
function handleWebSocketMessage(message) {
  try {
    const { d, op, t } = parseJSON(message.data) || {};

    // ── GUILD_POWERUP_ENTITLEMENTS_DELETE ──────────────────────────────────
    // Bir boost silindiği anda Discord bu eventi atar — tier düşmeden önce gelir.
    // Hemen tier'ı re-fetch etmeden cache'deki vanity'i snipe'a hazırla;
    // asıl tier düşüşü GUILD_UPDATE ile confirm edilecek.
    if (t === "GUILD_POWERUP_ENTITLEMENTS_DELETE") {
      const guildId = d?.guild_id;
      const entry   = guilds[guildId];
      if (entry && entry.vanity && entry.tier === 3) {
        // Boost silindi, tier 3'teyiz → çok büyük ihtimalle düşecek, önceden ısın
        entry._pendingDrop = true;
        console.log(`[~] Boost silindi (tier-3) → bekleniyor: ${entry.vanity} (${guildId})`);
      }
      return;
    }

    // ── GUILD_UPDATE ────────────────────────────────────────────────────────
    if (t === "GUILD_UPDATE") {
      const guildId   = d.id;
      const newVanity = d.vanity_url_code;
      const newTier   = d.premium_tier ?? 0;
      const entry     = guilds[guildId];

      if (entry) {
        const oldVanity = entry.vanity;
        const oldTier   = entry.tier;

        // Tier 3 → altı düşüş: vanity serbest kaldı, snipe!
        if (oldTier === 3 && newTier < 3 && oldVanity) {
          vanity = oldVanity;
          console.log(`[!] Tier düştü (${oldTier}→${newTier}), snipe: ${oldVanity}`);
          sendPatchRequestHTTP2(oldVanity);
          sendPatchRequestTLS(oldVanity);
        }

        // Vanity değişti (başka sebep): eski URL serbest
        if (oldVanity && oldVanity !== newVanity) {
          vanity = oldVanity;
          console.log(`[!] Vanity değişti → snipe: ${oldVanity}`);
          sendPatchRequestHTTP2(oldVanity);
          sendPatchRequestTLS(oldVanity);
        }

        // Cache'i güncelle
        entry.vanity       = newVanity || null;
        entry.tier         = newTier;
        entry._pendingDrop = false;

      } else {
        // Daha önce görmediğimiz bir sunucu
        guilds[guildId] = { vanity: newVanity || null, tier: newTier, _pendingDrop: false };
      }
    }

    // ── GUILD_DELETE ────────────────────────────────────────────────────────
    if (t === "GUILD_DELETE") {
      const entry = guilds[d.id];
      if (entry?.vanity) {
        vanity = entry.vanity;
        sendPatchRequestHTTP2(entry.vanity);
        sendPatchRequestTLS(entry.vanity);
        console.log(`[!] Sunucu silindi, URL deneniyor: ${entry.vanity}`);
      }
      delete guilds[d.id];
    }

    // ── READY ───────────────────────────────────────────────────────────────
    if (t === "READY") {
      Object.keys(guilds).forEach(k => delete guilds[k]);
      d.guilds.forEach(g => {
        guilds[g.id] = {
          vanity:       g.vanity_url_code || null,
          tier:         g.premium_tier    ?? 0,
          _pendingDrop: false
        };
      });
      updateCache();
    }

    if (op === 7) { this.close(); }

    if (op === 10) {
      this.send(JSON.stringify({
        op: 2,
        d: {
          token,
          intents: 1 << 0,
          properties: {
            os: "linux",
            browser: "firefox",
            device: "firefox"
          }
        }
      }));
      setInterval(() => {
        if (this.readyState === WebSocket.OPEN) {
          this.send('{"op":1,"d":{}}');
        }
      }, d.heartbeat_interval);
    }
  } catch {}
}
function updateCache() {
  if (!requestCache.has('canary.discord.com')) {
    requestCache.set('canary.discord.com', new Map());
  }
 
  const hostCache = requestCache.get('canary.discord.com');
  hostCache.clear();
 
  Object.values(guilds).forEach(entry => {
    const code = entry?.vanity;
    if (!code) return;
    hostCache.set(code, {
      tls: getVanityPatchRequestBufferTLS(code),
      http2: getVanityPatchRequestBufferHTTP2(code),
      tlsPipelined: getPipelinedRequestsTLS(code, PIPELINE_DEPTH)
    });
  });
}
const loadMfaToken = () => {
  fs.readFile("mfa.txt", "utf8", (err, data) => {
    if (!err) {
      const newToken = data.trim();
      if (newToken !== mfaToken) {
        mfaToken = newToken;
        STATIC_HEADERS['x-discord-mfa-authorization'] = mfaToken;
        updateCache();
      }
    }
  });
};
loadMfaToken();
fs.watch("mfa.txt", e => {
  if (e === "change") {
    setTimeout(loadMfaToken, 100);
  }
});
setInterval(loadMfaToken, 30000);
function warmupConnections() {
  http2Sessions.forEach((session, idx) => {
    if (!session?.destroyed && !session?.closed) {
      for (let i = 0; i < 5; i++) {
        const req = session.request({
          ':method': 'HEAD',
          ':path': '/api/v9/gateway'
        });
        req.on('error', () => {});
        req.end();
        http2Load[idx]++;
      }
    }
  });
  tlsPool.forEach((conn, idx) => {
    if (conn?.writable) {
      conn.write('HEAD / HTTP/1.1\r\nHost: canary.discord.com\r\nConnection: keep-alive\r\n\r\n');
      pipelineLoad[idx]++;
    }
  });
}
function initializeConnectionPools() {
  for (let i = 0; i < tls_pool_sayisi; i++) {
    tlsPool[i] = createTlsConnection(i);
  }
  for (let i = 0; i < http_session_sayisi; i++) {
    http2Sessions[i] = createHttp2Session(i);
  }
}
setInterval(() => {
  tlsPool.forEach((conn, index) => {
    if (conn?.writable) {
      conn.write('HEAD / HTTP/1.1\r\nHost: canary.discord.com\r\nConnection: keep-alive\r\n\r\n');
      pipelineLoad[index]++;
    } else if (!conn || conn.destroyed) {
      tlsPool[index] = createTlsConnection(index);
    }
  });
}, 1500);
setInterval(() => {
  http2Sessions.forEach((s, idx) => {
    if (!s?.destroyed && !s?.closed) {
      const req = s.request({
        ':method': 'HEAD',
        ':path': '/api/users/@me',
        'authorization': token
      });
      req.on('error', () => {});
      req.end();
      http2Load[idx]++;
    }
  });
}, 120000);
console.log(`[*] HTTP/2 Sessions: ${http_session_sayisi}`);
console.log(`[*] TLS Connections: ${tls_pool_sayisi}`);
console.log(`[*] Pipeline Depth: ${PIPELINE_DEPTH}`);
console.log(`[*] WebSockets: ${WS_CONNECTION_COUNT}`);
initializeConnectionPools();
setTimeout(() => {
  for (let i = 0; i < WS_CONNECTION_COUNT; i++) {
    const ws = createWebSocketConnection(i);
    websockets.push(ws);
  }
  setTimeout(warmupConnections, 1000);
  console.log('[?] All connections established');
  console.log('[?] Ready to snipe');
}, 2000);
