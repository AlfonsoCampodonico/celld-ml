(* JS cells: a QuickJS context per resident cell, storage on the cell's
   SQLite database. A class script defines [fetch(request, ctx)] and
   optionally [alarm(ctx)], either as globals or on [globalThis.default]:

     function fetch(request, ctx) {
       ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS hits(at INTEGER)");
       ctx.storage.sql.exec("INSERT INTO hits VALUES (?)", Date.now());
       return Response.json(ctx.storage.sql.exec("SELECT count(*) AS n FROM hits")[0]);
     } *)

type t

external create_raw : string -> string -> Celld_sqlite.db -> (t, string) result = "celld_js_create"
external dispose : t -> unit = "celld_js_dispose"

(* After a call left promise jobs queued, a context only answers with an
   error: dispose it and create a new one from the script. *)
external poisoned : t -> bool = "celld_js_poisoned"
external memory : t -> int = "celld_js_memory"

let prelude =
  {js|
"use strict";
// ---- bytes and encodings ----
globalThis.TextEncoder = class TextEncoder {
  get encoding() { return "utf-8"; }
  encode(s) {
    const out = [];
    for (const ch of String(s === undefined ? "" : s)) {
      const c = ch.codePointAt(0);
      if (c < 0x80) out.push(c);
      else if (c < 0x800) out.push(0xc0 | (c >> 6), 0x80 | (c & 63));
      else if (c < 0x10000) out.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 63), 0x80 | (c & 63));
      else out.push(0xf0 | (c >> 18), 0x80 | ((c >> 12) & 63), 0x80 | ((c >> 6) & 63), 0x80 | (c & 63));
    }
    return new Uint8Array(out);
  }
};
const __u8 = (b) => b instanceof Uint8Array ? b : b instanceof ArrayBuffer ? new Uint8Array(b) : ArrayBuffer.isView(b) ? new Uint8Array(b.buffer, b.byteOffset, b.byteLength) : new TextEncoder().encode(String(b));
globalThis.TextDecoder = class TextDecoder {
  get encoding() { return "utf-8"; }
  decode(buf) {
    if (buf === undefined || buf === null) return "";
    const b = __u8(buf);
    let s = "", i = 0;
    while (i < b.length) {
      let c = b[i++];
      if (c >= 0xf0) c = ((c & 7) << 18) | ((b[i++] & 63) << 12) | ((b[i++] & 63) << 6) | (b[i++] & 63);
      else if (c >= 0xe0) c = ((c & 15) << 12) | ((b[i++] & 63) << 6) | (b[i++] & 63);
      else if (c >= 0xc0) c = ((c & 31) << 6) | (b[i++] & 63);
      s += String.fromCodePoint(c);
    }
    return s;
  }
};
// ---- WebSockets (the Durable Object hibernation API) ----
// Sockets live in the node; a cell sees them by id. Each call starts from
// the ids the node reports open and returns what to send and close, which
// the node applies once the call's writes are durable.
const __celld_ws = { open: new Set(), accepted: [], sends: [], closes: [] };
const __b64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const __to_b64 = (bytes) => {
  let out = "";
  for (let i = 0; i < bytes.length; i += 3) {
    const v = (bytes[i] << 16) | ((bytes[i + 1] || 0) << 8) | (bytes[i + 2] || 0);
    out += __b64[(v >> 18) & 63] + __b64[(v >> 12) & 63] + (i + 1 < bytes.length ? __b64[(v >> 6) & 63] : "=") + (i + 2 < bytes.length ? __b64[v & 63] : "=");
  }
  return out;
};
const __from_b64 = (s) => {
  s = String(s).replace(/[^A-Za-z0-9+/=]/g, "");
  const clean = s.replace(/=+$/, "");
  const out = new Uint8Array(Math.floor((clean.length * 3) / 4));
  let o = 0;
  for (let i = 0; i < clean.length; i += 4) {
    const v = (__b64.indexOf(clean[i]) << 18) | (__b64.indexOf(clean[i + 1]) << 12) | ((__b64.indexOf(clean[i + 2] || "A") & 63) << 6) | (__b64.indexOf(clean[i + 3] || "A") & 63);
    if (o < out.length) out[o++] = (v >> 16) & 255;
    if (o < out.length) out[o++] = (v >> 8) & 255;
    if (o < out.length) out[o++] = v & 255;
  }
  return out.buffer;
};
const __ws_table = () => __celld_sql_exec("CREATE TABLE IF NOT EXISTS __celld_ws(id TEXT PRIMARY KEY, tags TEXT NOT NULL, attachment TEXT)", []);
class __CelldWebSocket {
  constructor(id) { this.id = id; this.readyState = __celld_ws.open.has(id) ? 1 : 0; }
  send(data) {
    if (typeof data === "string") __celld_ws.sends.push({ id: this.id, data, binary: false });
    else __celld_ws.sends.push({ id: this.id, data: __to_b64(new Uint8Array(data.buffer ? data.buffer : data)), binary: true });
  }
  close(code, reason) { __celld_ws.closes.push({ id: this.id, code: code || 1000, reason: reason || "" }); this.readyState = 3; }
  serializeAttachment(value) { __ws_table(); __celld_sql_exec("UPDATE __celld_ws SET attachment = ? WHERE id = ?", [JSON.stringify(value), this.id]); }
  deserializeAttachment() {
    __ws_table();
    const r = __celld_sql_exec("SELECT attachment FROM __celld_ws WHERE id = ?", [this.id]);
    return r.length && r[0].attachment != null ? JSON.parse(r[0].attachment) : null;
  }
}
globalThis.WebSocketPair = class WebSocketPair {
  constructor() {
    const id = __celld_sql_exec("SELECT lower(hex(randomblob(12))) AS id", [])[0].id;
    this[0] = new __CelldWebSocket(id);
    this[1] = new __CelldWebSocket(id);
  }
};
const __celld_outbox = (queue, messages) => {
  if (typeof queue !== "string" || !/^[A-Za-z0-9_.$-]{1,200}$/.test(queue)) throw new TypeError("bad queue name");
  if (messages.length > 100) throw new RangeError("sendBatch takes at most 100 messages");
  __celld_sql_exec("CREATE TABLE IF NOT EXISTS __celld_outbox(id TEXT PRIMARY KEY, queue TEXT NOT NULL, payload TEXT NOT NULL)", []);
  let total = 0;
  for (const m of messages) {
    const opts = m.options || {};
    const type = opts.contentType || (typeof m.body === "string" ? "text" : "json");
    const body = type === "text" ? String(m.body) : JSON.stringify(m.body);
    if (body.length > 128000) throw new RangeError("a message is at most 128000 bytes");
    total += body.length;
    if (total > 256000) throw new RangeError("a batch is at most 256000 bytes");
    const delay = opts.delaySeconds || 0;
    if (delay < 0 || delay > 86400) throw new RangeError("delaySeconds is 0 to 86400");
    const id = __celld_sql_exec("SELECT lower(hex(randomblob(16))) AS id", [])[0].id;
    const payload = JSON.stringify({ id, body, content_type: type, delay_ms: Math.round(delay * 1000) });
    __celld_sql_exec("INSERT INTO __celld_outbox VALUES (?, ?, ?)", [id, queue, payload]);
  }
};
const __celld_kv = () => {
  __celld_sql_exec("CREATE TABLE IF NOT EXISTS __celld_kv(k TEXT PRIMARY KEY, v TEXT NOT NULL)");
};
globalThis.btoa = (s) => __to_b64(Array.from(String(s), (c) => { const x = c.charCodeAt(0); if (x > 255) throw new TypeError("btoa: not Latin-1"); return x; }));
globalThis.atob = (s) => { const b = new Uint8Array(__from_b64(s)); let out = ""; for (let i = 0; i < b.length; i++) out += String.fromCharCode(b[i]); return out; };
const __fmt = (args) => args.map((a) => typeof a === "string" ? a : (() => { try { return a instanceof Error ? String(a) : JSON.stringify(a); } catch (e) { return String(a); } })()).join(" ");
globalThis.console = {
  log: (...a) => __celld_log("info", __fmt(a)), info: (...a) => __celld_log("info", __fmt(a)), debug: (...a) => __celld_log("debug", __fmt(a)),
  warn: (...a) => __celld_log("warn", __fmt(a)), error: (...a) => __celld_log("error", __fmt(a)),
};
globalThis.structuredClone = (v) => v === undefined ? undefined : JSON.parse(JSON.stringify(v));
globalThis.queueMicrotask = (f) => { Promise.resolve().then(f); };
globalThis.performance = { now: () => Date.now(), timeOrigin: Date.now() };

// ---- async operations ----
// QuickJS cannot block, so I/O suspends the call: the node performs the
// queued operations and resumes it with their results.
const __celld_ops = { next: 1, count: 0, waiting: new Map(), queued: [] };
const __celld_op = (kind, args) => new Promise((resolve, reject) => {
  if (++__celld_ops.count > 50) { reject(new Error("a call makes at most 50 subrequests")); return; }
  const id = __celld_ops.next++;
  __celld_ops.waiting.set(id, { resolve, reject });
  // _op is the envelope's own id; an operation may have an "id" argument.
  __celld_ops.queued.push(Object.assign({}, args, { _op: id, kind }));
});
globalThis.__celld_take_ops = () => {
  if (!__celld_ops.queued.length) return "";
  const q = __celld_ops.queued;
  __celld_ops.queued = [];
  return JSON.stringify(q);
};
globalThis.__celld_resume = (json) => {
  for (const r of JSON.parse(json)) {
    if (r._op === 0) { for (const w of __celld_ops.waiting.values()) w.reject(new Error(r.error)); __celld_ops.waiting.clear(); continue; }
    const w = __celld_ops.waiting.get(r._op);
    if (!w) continue;
    __celld_ops.waiting.delete(r._op);
    if (r.error !== undefined) w.reject(new Error(r.error)); else w.resolve(r.value);
  }
};
const __timers = new Map();
let __timer_next = 1;
globalThis.setTimeout = (f, ms, ...args) => {
  const id = __timer_next++;
  __timers.set(id, true);
  __celld_op("sleep", { ms: Math.max(0, Math.min(Number(ms) || 0, 30000)) }).then(() => { if (__timers.delete(id)) f(...args); });
  return id;
};
globalThis.clearTimeout = (id) => { __timers.delete(id); };
globalThis.setInterval = (f, ms, ...args) => {
  const id = __timer_next++;
  __timers.set(id, true);
  const tick = () => __celld_op("sleep", { ms: Math.max(1, Math.min(Number(ms) || 0, 30000)) }).then(() => { if (__timers.has(id)) { f(...args); tick(); } });
  tick();
  return id;
};
globalThis.clearInterval = globalThis.clearTimeout;

// ---- streams (minimal) ----
globalThis.ReadableStream = class ReadableStream {
  constructor(src) {
    src = src || {};
    this._chunks = []; this._closed = false; this._error = null; this.locked = false;
    const c = { enqueue: (x) => this._chunks.push(x), close: () => { this._closed = true; }, error: (e) => { this._error = e; } };
    this._pull = src.pull ? () => src.pull(c) : null;
    if (src.start) src.start(c);
  }
  static __from(bytes) { return new ReadableStream({ start(c) { c.enqueue(bytes); c.close(); } }); }
  getReader() {
    const s = this;
    s.locked = true;
    return {
      async read() {
        while (!s._chunks.length && !s._closed && !s._error && s._pull) await s._pull();
        if (s._error) throw s._error;
        if (s._chunks.length) return { value: s._chunks.shift(), done: false };
        return { value: undefined, done: true };
      },
      releaseLock() { s.locked = false; },
      async cancel() { s._closed = true; s._chunks = []; },
    };
  }
  async *[Symbol.asyncIterator]() { const r = this.getReader(); for (;;) { const x = await r.read(); if (x.done) return; yield x.value; } }
  async __bytes() {
    const parts = [];
    for await (const c of this) parts.push(__u8(c));
    const n = parts.reduce((a, p) => a + p.length, 0), out = new Uint8Array(n);
    let o = 0;
    for (const p of parts) { out.set(p, o); o += p.length; }
    return out;
  }
};

// ---- Headers, Request, Response ----
globalThis.Headers = class Headers {
  constructor(init) {
    this._m = new Map();
    if (init instanceof Headers) init.forEach((v, k) => this.set(k, v));
    else if (Array.isArray(init)) for (const [k, v] of init) this.append(k, v);
    else if (init) for (const k of Object.keys(init)) this.set(k, init[k]);
  }
  get(k) { const v = this._m.get(String(k).toLowerCase()); return v === undefined ? null : v; }
  set(k, v) { this._m.set(String(k).toLowerCase(), String(v)); }
  append(k, v) { const key = String(k).toLowerCase(), o = this._m.get(key); this._m.set(key, o === undefined ? String(v) : o + ", " + v); }
  has(k) { return this._m.has(String(k).toLowerCase()); }
  delete(k) { this._m.delete(String(k).toLowerCase()); }
  forEach(f) { for (const [k, v] of this._m) f(v, k, this); }
  entries() { return this._m.entries(); }
  keys() { return this._m.keys(); }
  values() { return this._m.values(); }
  [Symbol.iterator]() { return this._m.entries(); }
};
const __body_init = (o, body) => {
  o.bodyUsed = false;
  o._stream = body instanceof ReadableStream ? body : null;
  o._bytes = body === undefined || body === null || o._stream ? null : body instanceof URLSearchParams ? new TextEncoder().encode(body.toString()) : __u8(body);
};
class __Body {
  get body() { return this._stream || (this._bytes ? ReadableStream.__from(this._bytes) : null); }
  async bytes() {
    if (this.bodyUsed) throw new TypeError("body already used");
    this.bodyUsed = true;
    if (this._stream) this._bytes = await this._stream.__bytes();
    return this._bytes || new Uint8Array(0);
  }
  async arrayBuffer() { const b = await this.bytes(); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); }
  async text() { return new TextDecoder().decode(await this.bytes()); }
  async json() { return JSON.parse(await this.text()); }
}
globalThis.Response = class Response extends __Body {
  constructor(body, init) {
    super();
    init = init || {};
    __body_init(this, body);
    this.status = init.status || 200;
    this.statusText = init.statusText || "";
    this.headers = new Headers(init.headers);
    this.webSocket = init.webSocket || null;
  }
  get ok() { return this.status >= 200 && this.status < 300; }
  clone() { return new Response(this._bytes, { status: this.status, headers: this.headers }); }
  static json(value, init) {
    init = init || {};
    const headers = new Headers(init.headers);
    if (!headers.has("content-type")) headers.set("content-type", "application/json");
    return new Response(JSON.stringify(value), { status: init.status, headers });
  }
  static redirect(url, status) { return new Response(null, { status: status || 302, headers: { location: String(url) } }); }
};
globalThis.Request = class Request extends __Body {
  constructor(input, init) {
    super();
    init = init || {};
    const from = input instanceof Request ? input : null;
    this.url = from ? from.url : String(input);
    this.method = String(init.method || (from && from.method) || "GET").toUpperCase();
    this.headers = new Headers(init.headers || (from && from.headers));
    __body_init(this, init.body !== undefined ? init.body : from ? from._bytes : null);
  }
  clone() { return new Request(this.url, { method: this.method, headers: this.headers, body: this._bytes }); }
};
globalThis.URLSearchParams = class URLSearchParams {
  constructor(init) {
    this._p = [];
    if (typeof init === "string") {
      const s = init.startsWith("?") ? init.slice(1) : init;
      if (s) for (const kv of s.split("&")) {
        const i = kv.indexOf("=");
        const dec = (x) => decodeURIComponent(x.replace(/\+/g, " "));
        this._p.push([dec(i < 0 ? kv : kv.slice(0, i)), i < 0 ? "" : dec(kv.slice(i + 1))]);
      }
    } else if (init) for (const [k, v] of Array.isArray(init) ? init : Object.entries(init)) this._p.push([String(k), String(v)]);
  }
  get(k) { const e = this._p.find((p) => p[0] === k); return e ? e[1] : null; }
  getAll(k) { return this._p.filter((p) => p[0] === k).map((p) => p[1]); }
  set(k, v) { this.delete(k); this._p.push([k, String(v)]); }
  append(k, v) { this._p.push([k, String(v)]); }
  has(k) { return this._p.some((p) => p[0] === k); }
  delete(k) { this._p = this._p.filter((p) => p[0] !== k); }
  forEach(f) { this._p.forEach(([k, v]) => f(v, k, this)); }
  entries() { return this._p[Symbol.iterator](); }
  [Symbol.iterator]() { return this._p[Symbol.iterator](); }
  toString() { return this._p.map(([k, v]) => encodeURIComponent(k) + "=" + encodeURIComponent(v)).join("&"); }
};
globalThis.URL = class URL {
  constructor(u, base) {
    let s = String(u);
    if (base !== undefined && !/^[a-z][a-z0-9+.-]*:/i.test(s)) {
      const b = new URL(base);
      s = s.startsWith("/") ? b.origin + s : b.origin + b.pathname.replace(/[^/]*$/, "") + s;
    }
    const m = s.match(/^([a-z][a-z0-9+.-]*:)\/\/(?:([^:@/]*)(?::([^@/]*))?@)?(\[[^\]]+\]|[^:/?#]*)(?::(\d+))?([^?#]*)(\?[^#]*)?(#.*)?$/i);
    if (!m) throw new TypeError("Invalid URL: " + s);
    this.protocol = m[1].toLowerCase(); this.username = m[2] || ""; this.password = m[3] || "";
    this.hostname = m[4].toLowerCase(); this.port = m[5] || ""; this.pathname = m[6] || "/"; this.hash = m[8] || "";
    this.searchParams = new URLSearchParams(m[7] || "");
  }
  get search() { const q = this.searchParams.toString(); return q ? "?" + q : ""; }
  get host() { return this.hostname + (this.port ? ":" + this.port : ""); }
  get origin() { return this.protocol + "//" + this.host; }
  get href() { return this.origin + this.pathname + this.search + this.hash; }
  toString() { return this.href; }
  toJSON() { return this.href; }
};

// ---- outbound fetch ----
const __outbound_fetch = async (input, init) => {
  const req = input instanceof Request && !init ? input : new Request(input instanceof URL ? input.href : input, init);
  const body = await req.bytes();
  const v = await __celld_op("fetch", { url: req.url, method: req.method, headers: Object.fromEntries(req.headers), body: __to_b64(body) });
  return new Response(v.status === 204 || v.status === 304 ? null : new Uint8Array(__from_b64(v.body)), { status: v.status, headers: v.headers });
};
globalThis.fetch = __outbound_fetch;

// ---- Web Crypto (random, digest, HMAC) ----
const __hash_name = (a) => { const n = String(typeof a === "string" ? a : a && a.name).toUpperCase(); return { "SHA-1": "sha1", "SHA-256": "sha256", "SHA-384": "sha384", "SHA-512": "sha512", "MD5": "md5" }[n] || n; };
class CryptoKey { constructor(algorithm, raw, usages) { this.type = "secret"; this.extractable = true; this.algorithm = algorithm; this.usages = usages || []; this._raw = raw; } }
globalThis.CryptoKey = CryptoKey;
globalThis.crypto = {
  getRandomValues(a) {
    if (!ArrayBuffer.isView(a)) throw new TypeError("getRandomValues needs a typed array");
    new Uint8Array(a.buffer, a.byteOffset, a.byteLength).set(new Uint8Array(__celld_random(a.byteLength)));
    return a;
  },
  randomUUID() {
    const b = new Uint8Array(__celld_random(16));
    b[6] = (b[6] & 0x0f) | 0x40; b[8] = (b[8] & 0x3f) | 0x80;
    const h = Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
    return h.slice(0, 8) + "-" + h.slice(8, 12) + "-" + h.slice(12, 16) + "-" + h.slice(16, 20) + "-" + h.slice(20);
  },
  subtle: {
    async digest(alg, data) { return __celld_digest(__hash_name(alg), __u8(data)); },
    async importKey(format, key, alg, extractable, usages) {
      if (format !== "raw") throw new TypeError("only raw HMAC keys are supported");
      const a = typeof alg === "string" ? { name: alg } : alg;
      if (String(a.name).toUpperCase() !== "HMAC") throw new TypeError("only HMAC keys are supported");
      return new CryptoKey({ name: "HMAC", hash: { name: typeof a.hash === "string" ? a.hash : a.hash.name } }, new Uint8Array(__u8(key)), usages);
    },
    async generateKey(alg, extractable, usages) {
      const a = typeof alg === "string" ? { name: alg } : alg;
      return new CryptoKey({ name: "HMAC", hash: { name: typeof a.hash === "string" ? a.hash : a.hash.name } }, new Uint8Array(__celld_random((a.length || 256) / 8)), usages);
    },
    async exportKey(format, key) { if (format !== "raw") throw new TypeError("only raw export"); return key._raw.buffer.slice(0); },
    async sign(alg, key, data) { return __celld_hmac(__hash_name(key.algorithm.hash), key._raw, __u8(data)); },
    async verify(alg, key, sig, data) {
      const want = new Uint8Array(__celld_hmac(__hash_name(key.algorithm.hash), key._raw, __u8(data))), got = __u8(sig);
      if (want.length !== got.length) return false;
      let d = 0;
      for (let i = 0; i < want.length; i++) d |= want[i] ^ got[i];
      return d === 0;
    },
    timingSafeEqual(a, b) { const x = __u8(a), y = __u8(b); if (x.length !== y.length) return false; let d = 0; for (let i = 0; i < x.length; i++) d |= x[i] ^ y[i]; return d === 0; },
  },
};

const __wait_until = [];
const __stub_response = (v) => new Response(v.status === 204 ? null : new Uint8Array(__from_b64(v.body)), { status: v.status, headers: v.headers });
const __celld_ctx = {
  waitUntil(p) { __wait_until.push(Promise.resolve(p)); },
  // A Durable Object-style stub for another cell.
  cell(id) {
    return {
      id,
      async fetch(input, init) {
        const req = input instanceof Request && !init ? input : new Request(String(input).startsWith("/") ? "http://cell" + input : input, init);
        const u = new URL(req.url);
        const body = await req.bytes();
        return __stub_response(await __celld_op("cell", { cell: id, method: req.method, path: u.pathname + u.search, body: __to_b64(body) }));
      },
    };
  },
  // D1: a SQL database in its own cell (D1:<name>).
  d1(name) {
    const run = (stmts, batch) => __celld_op("d1", { db: name, stmts, batch: !!batch });
    const statement = (sql, params) => ({
      bind: (...p) => statement(sql, p),
      async all() { const r = await run([{ sql, params }]); return { results: r[0].rows, success: true, meta: r[0].meta }; },
      async first(col) { const r = await run([{ sql, params }]); const row = r[0].rows[0]; return row === undefined ? null : col === undefined ? row : row[col]; },
      async run() { const r = await run([{ sql, params }]); return { success: true, meta: r[0].meta, results: r[0].rows }; },
      async raw() { const r = await run([{ sql, params }]); return r[0].rows.map((row) => Object.values(row)); },
      __s: { sql, params },
    });
    return {
      prepare: (sql) => statement(sql, []),
      async batch(stmts) { const r = await run(stmts.map((s) => s.__s), true); return r.map((x) => ({ results: x.rows, success: true, meta: x.meta })); },
      async exec(sql) { const r = await run(sql.split(";").map((s) => s.trim()).filter(Boolean).map((s) => ({ sql: s, params: [] })), true); return { count: r.length }; },
    };
  },
  // R2: objects in the fleet's bucket, under r2/<bucket>/.
  r2(bucket) {
    const object = (key, v) => {
      if (!v) return null;
      const bytes = v.body !== undefined ? new Uint8Array(__from_b64(v.body)) : null;
      return {
        key, size: v.size, etag: v.etag, httpEtag: '"' + v.etag + '"', uploaded: new Date(v.uploaded || 0),
        httpMetadata: v.httpMetadata || {}, customMetadata: v.customMetadata || {},
        body: bytes && ReadableStream.__from(bytes),
        async arrayBuffer() { return bytes.buffer.slice(0); },
        async text() { return new TextDecoder().decode(bytes); },
        async json() { return JSON.parse(new TextDecoder().decode(bytes)); },
      };
    };
    return {
      async get(key) { return object(key, await __celld_op("r2", { op: "get", bucket, key })); },
      async head(key) { const v = await __celld_op("r2", { op: "head", bucket, key }); if (v) delete v.body; return object(key, v); },
      async put(key, value, opts) {
        const bytes = value instanceof ReadableStream ? await value.__bytes() : __u8(value === undefined || value === null ? "" : value);
        const v = await __celld_op("r2", { op: "put", bucket, key, body: __to_b64(bytes), httpMetadata: (opts && opts.httpMetadata) || {}, customMetadata: (opts && opts.customMetadata) || {} });
        return object(key, Object.assign({}, v, { body: undefined }));
      },
      async delete(keys) { for (const key of Array.isArray(keys) ? keys : [keys]) await __celld_op("r2", { op: "delete", bucket, key }); },
      async list(opts) {
        opts = opts || {};
        const v = await __celld_op("r2", { op: "list", bucket, prefix: opts.prefix || "", limit: opts.limit || 1000, cursor: opts.cursor || "" });
        return { objects: v.keys.map((k) => ({ key: k })), truncated: v.truncated, cursor: v.cursor, delimitedPrefixes: [] };
      },
    };
  },
  // Workflows: durable runs of a class's run(event, step), one cell each.
  workflow(className) {
    const handle = (id) => ({
      id,
      async status() { return await __celld_op("workflow", { class: className, id, action: "status" }); },
    });
    return {
      async create(opts) {
        opts = opts || {};
        const id = opts.id || crypto.randomUUID();
        await __celld_op("workflow", { class: className, id, action: "create", params: opts.params === undefined ? null : opts.params });
        return handle(id);
      },
      async get(id) { return handle(id); },
    };
  },
  acceptWebSocket(ws, tags) {
    __ws_table();
    __celld_sql_exec("INSERT OR REPLACE INTO __celld_ws(id, tags) VALUES (?, ?)", [ws.id, JSON.stringify(tags || [])]);
    __celld_ws.accepted.push(ws.id);
    __celld_ws.open.add(ws.id);
    ws.readyState = 1;
  },
  getWebSockets(tag) {
    __ws_table();
    const tagged = tag === undefined ? null : new Set(__celld_sql_exec("SELECT __celld_ws.id AS id FROM __celld_ws, json_each(__celld_ws.tags) WHERE json_each.value = ?", [tag]).map((r) => r.id));
    return [...__celld_ws.open].filter((id) => !tagged || tagged.has(id)).map((id) => new __CelldWebSocket(id));
  },
  getTags(ws) { __ws_table(); const r = __celld_sql_exec("SELECT tags FROM __celld_ws WHERE id = ?", [ws.id]); return r.length ? JSON.parse(r[0].tags) : []; },
  // Messages go to this cell's outbox in the same commit as the rest of the
  // request, and reach the queue once that commit is durable.
  queue: (name) => ({
    send: async (body, options) => __celld_outbox(name, [{ body, options }]),
    sendBatch: async (messages) => __celld_outbox(name, messages.map((m) => ({ body: m.body, options: m }))),
  }),
  storage: {
    sql: { exec: (query, ...bindings) => __celld_sql_exec(query, bindings) },
    get(key) {
      __celld_kv();
      const r = __celld_sql_exec("SELECT v FROM __celld_kv WHERE k = ?", [key]);
      return r.length ? JSON.parse(r[0].v) : undefined;
    },
    put(key, value) {
      __celld_kv();
      __celld_sql_exec("INSERT OR REPLACE INTO __celld_kv VALUES (?, ?)", [key, JSON.stringify(value)]);
    },
    delete(key) {
      __celld_kv();
      return __celld_sql_exec("DELETE FROM __celld_kv WHERE k = ? RETURNING k", [key]).length > 0;
    },
    list() {
      __celld_kv();
      const out = new Map();
      for (const r of __celld_sql_exec("SELECT k, v FROM __celld_kv ORDER BY k", [])) out.set(r.k, JSON.parse(r.v));
      return out;
    },
  },
};
const __ws_ops = () => {
  const w = __celld_ws;
  return w.accepted.length || w.sends.length || w.closes.length ? JSON.stringify({ accept: w.accepted, sends: w.sends, closes: w.closes }) : "";
};
// The class's own fetch handler, set aside so global fetch stays outbound.
const __user = {};
globalThis.__celld_after_load = () => {
  if (globalThis.fetch !== __outbound_fetch) { __user.fetch = globalThis.fetch; globalThis.fetch = __outbound_fetch; }
};
const __handler = (name) => {
  const d = globalThis.default;
  if (d && typeof d[name] === "function") return [d, d[name]];
  if (name === "fetch") return [globalThis, __user.fetch];
  return [globalThis, globalThis[name]];
};
// Output: status, headers (JSON), socket effects (JSON or empty), body (base64).
const __out = (status, headers, body) => status + "\n" + JSON.stringify(headers) + "\n" + __ws_ops() + "\n" + __to_b64(__u8(body === undefined || body === null ? "" : body));
const __json_out = (status, value) => __out(status, { "content-type": "application/json" }, JSON.stringify(value));
class __Suspend { constructor(at) { this.at = at; } }
const __duration = (d) => {
  if (typeof d === "number") return d;
  const m = String(d).trim().match(/^(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|seconds?|m|minutes?|h|hours?|d|days?)?$/i);
  if (!m) throw new TypeError("bad duration " + d);
  const n = Number(m[1]), u = (m[2] || "ms").toLowerCase();
  return Math.round(n * (u.startsWith("ms") || u.startsWith("milli") ? 1 : u.startsWith("s") ? 1000 : u.startsWith("m") ? 60000 : u.startsWith("h") ? 3600000 : 86400000));
};
const __wf_table = () => __celld_sql_exec("CREATE TABLE IF NOT EXISTS __celld_wf_steps(name TEXT PRIMARY KEY, done INTEGER NOT NULL DEFAULT 0, result TEXT, attempts INTEGER NOT NULL DEFAULT 0, wake INTEGER)", []);
const __step_row = (name) => { __wf_table(); return __celld_sql_exec("SELECT done, result, attempts, wake FROM __celld_wf_steps WHERE name = ?", [name])[0]; };
const __wf_step = {
  async do(name, a, b) {
    const fn = typeof a === "function" ? a : b, cfg = typeof a === "function" ? {} : a || {};
    const row = __step_row(name);
    if (row && row.done) return row.result === null ? undefined : JSON.parse(row.result);
    if (row && row.wake && row.wake > Date.now()) throw new __Suspend(row.wake);
    __celld_sql_exec("SAVEPOINT celld_step", []);
    try {
      const v = await fn({ attempt: ((row && row.attempts) || 0) + 1 });
      __celld_sql_exec("RELEASE celld_step", []);
      __celld_sql_exec("INSERT OR REPLACE INTO __celld_wf_steps(name, done, result, attempts) VALUES (?, 1, ?, ?)", [name, v === undefined ? null : JSON.stringify(v), ((row && row.attempts) || 0) + 1]);
      return v;
    } catch (e) {
      if (e instanceof __Suspend) { __celld_sql_exec("RELEASE celld_step", []); throw e; }
      __celld_sql_exec("ROLLBACK TO celld_step", []); __celld_sql_exec("RELEASE celld_step", []);
      const attempts = ((row && row.attempts) || 0) + 1, r = cfg.retries || {};
      const limit = r.limit === undefined ? 5 : r.limit;
      if (attempts > limit) throw e;
      const base = r.delay === undefined ? 1000 : __duration(r.delay);
      const delay = r.backoff === "constant" ? base : r.backoff === "linear" ? base * attempts : base * Math.pow(2, attempts - 1);
      const wake = Date.now() + delay;
      __celld_sql_exec("INSERT OR REPLACE INTO __celld_wf_steps(name, done, attempts, wake) VALUES (?, 0, ?, ?)", [name, attempts, wake]);
      console.warn("workflow step " + name + " failed (attempt " + attempts + "), retrying: " + String(e));
      throw new __Suspend(wake);
    }
  },
  async sleep(name, d) { return __wf_step.sleepUntil(name, null, __duration(d)); },
  async sleepUntil(name, ts, ms) {
    const row = __step_row(name);
    if (row && row.done) return;
    const wake = row && row.wake ? row.wake : ts !== null ? new Date(ts).getTime() : Date.now() + ms;
    if (Date.now() < wake) {
      if (!row) __celld_sql_exec("INSERT INTO __celld_wf_steps(name, done, attempts, wake) VALUES (?, 0, 0, ?)", [name, wake]);
      throw new __Suspend(wake);
    }
    __celld_sql_exec("INSERT OR REPLACE INTO __celld_wf_steps(name, done, attempts) VALUES (?, 1, 0)", [name]);
  },
};
globalThis.__celld_dispatch = async (name, input) => {
  const r = JSON.parse(input);
  __celld_ctx.id = r.cell;
  __celld_ops.count = 0; __celld_ops.waiting = new Map(); __celld_ops.queued = [];
  __wait_until.length = 0;
  __celld_ws.open = new Set(r.sockets || []);
  __celld_ws.accepted = []; __celld_ws.sends = []; __celld_ws.closes = [];
  const out = await __celld_dispatch_inner(name, r);
  // waitUntil work finishes within the call, before its output leaves.
  if (__wait_until.length) await Promise.allSettled(__wait_until);
  return out;
};
const __celld_dispatch_inner = async (name, r) => {
  const body = () => r.body_b64 === undefined ? r.body : new TextDecoder().decode(new Uint8Array(__from_b64(r.body_b64)));
  if (name === "ws_message" || name === "ws_close") {
    const ev = JSON.parse(body());
    const [self] = __handler("fetch");
    const ws = new __CelldWebSocket(ev.id);
    if (name === "ws_close") {
      __celld_ws.open.delete(ev.id);
      __ws_table();
      // The handler may still read the socket's attachment and tags.
      const [o, h] = __handler("webSocketClose");
      if (typeof h === "function") await h.call(o, ws, ev.code, ev.reason, ev.code !== 1006, __celld_ctx);
      __celld_sql_exec("DELETE FROM __celld_ws WHERE id = ?", [ev.id]);
    } else {
      const [o, h] = __handler("webSocketMessage");
      if (typeof h === "function") await h.call(o, ws, ev.binary ? __from_b64(ev.data) : ev.data, __celld_ctx);
    }
    return __out(200, {}, "");
  }
  if (name === "workflow") {
    const ev = JSON.parse(body());
    const [o, h] = __handler("run");
    if (typeof h !== "function") return __json_out(200, { status: "errored", error: "this class has no run(event, step) handler" });
    try {
      const output = await h.call(o, { payload: ev.params, instanceId: ev.id, timestamp: new Date(ev.created) }, __wf_step, __celld_ctx);
      return __json_out(200, { status: "complete", output: output === undefined ? null : output });
    } catch (e) {
      if (e instanceof __Suspend) return __json_out(200, { status: "waiting", wake: e.at });
      return __json_out(200, { status: "errored", error: String(e) });
    }
  }
  const [self, h] = __handler(name);
  if (typeof h !== "function")
    return name === "alarm" ? __out(200, {}, "") : __out(501, { "content-type": "text/plain" }, "this class has no " + name + " handler\n");
  if (name === "scheduled") {
    const input = JSON.parse(body());
    let noRetry = false;
    const controller = { cron: input.cron, scheduledTime: input.scheduledTime, noRetry() { noRetry = true; } };
    try {
      await h.call(self, controller, __celld_ctx);
      return __json_out(200, {});
    } catch (e) {
      return __json_out(500, { error: String(e) + (e && e.stack ? "\n" + e.stack : ""), noRetry });
    }
  }
  if (name === "queue") {
    // A handler that throws has its writes rolled back and its whole batch
    // retried, acknowledged or not, so no message is lost with its effects.
    const input = JSON.parse(body());
    const settled = new Map();
    const messages = input.messages.map((m) => ({
      id: m.id,
      timestamp: new Date(m.timestamp),
      attempts: m.attempts,
      body: m.contentType === "text" ? m.body : JSON.parse(m.body),
      ack() { if (!settled.has(m.id)) settled.set(m.id, { ack: true }); },
      retry(o) { if (!settled.has(m.id)) settled.set(m.id, { ack: false, delay_ms: Math.round(((o && o.delaySeconds) || 0) * 1000) }); },
    }));
    const batch = { queue: input.queue, messages, ackAll() { messages.forEach((m) => m.ack()); }, retryAll(o) { messages.forEach((m) => m.retry(o)); } };
    try {
      await h.call(self, batch, __celld_ctx);
    } catch (e) {
      return __json_out(500, { error: String(e) + (e && e.stack ? "\n" + e.stack : "") });
    }
    const ack = [], retry = [];
    for (const m of input.messages) {
      const s = settled.get(m.id) || { ack: true };
      if (s.ack) ack.push(m.id); else retry.push({ id: m.id, delay_ms: s.delay_ms });
    }
    return __json_out(200, { ack, retry });
  }
  let out;
  if (name === "alarm") out = await h.call(self, __celld_ctx);
  else {
    const qs = Object.entries(r.query).map(([k, v]) => encodeURIComponent(k) + "=" + encodeURIComponent(v)).join("&");
    // The URL the client used: its Host, else a placeholder (the cell id is
    // not a valid host).
    const host = (r.headers && r.headers.host) || "cell.celld";
    const request = new Request("http://" + host + r.path + (qs ? "?" + qs : ""), {
      method: r.method, headers: r.headers || {}, body: r.body_b64 === undefined ? (r.body || null) : new Uint8Array(__from_b64(r.body_b64)),
    });
    request.path = r.path;
    request.query = r.query;
    out = await h.call(self, request, __celld_ctx);
  }
  if (!(out instanceof Response)) out = typeof out === "string" ? new Response(out) : Response.json(out === undefined ? null : out);
  if (out.status === 101 && !__celld_ws.accepted.length) return __out(500, { "content-type": "text/plain" }, "a 101 response needs ctx.acceptWebSocket(server) first\n");
  const headers = Object.fromEntries(out.headers);
  if (!headers["content-type"]) headers["content-type"] = "text/plain";
  return __out(out.status, headers, out.status === 101 ? "" : await out.bytes());
};
|js}

(* After a class script loads, its own fetch handler is set aside so the
   global fetch stays the outbound one. *)
let create ~script ~db = create_raw prelude (script ^ "\n;__celld_after_load();\n") db

type response = {
  status : int;
  headers : (string * string) list;
  body : string;
  effects : string;  (** socket operations, JSON, or "" *)
}

let content_type r = Option.value (List.assoc_opt "content-type" r.headers) ~default:"text/plain"

(* A call either finishes or waits for operations (JSON array of
   {id, kind, ...}) that the node performs and passes to [resume]. *)
type step = Reply of response | Await of string

(* Open socket ids of the cell being called, set by the node before each
   call (calls are serial). *)
let sockets : string list ref = ref []
let headers : (string * string) list ref = ref []

(* Natives backed by OCaml. The node replaces the random source and the
   logger; the defaults serve native tests. *)
let random_source : (int -> string) ref = ref (fun n -> String.init n (fun _ -> Char.chr (Random.int 256)))
let logger : (string -> string -> unit) ref = ref (fun level msg -> prerr_endline ("[js " ^ level ^ "] " ^ msg))

let digest alg data =
  let open Digestif in
  match alg with
  | "sha1" -> SHA1.(to_raw_string (digest_string data))
  | "sha256" -> SHA256.(to_raw_string (digest_string data))
  | "sha384" -> SHA384.(to_raw_string (digest_string data))
  | "sha512" -> SHA512.(to_raw_string (digest_string data))
  | "md5" -> MD5.(to_raw_string (digest_string data))
  | _ -> ""

let hmac alg key data =
  let open Digestif in
  match alg with
  | "sha1" -> SHA1.(to_raw_string (hmac_string ~key data))
  | "sha256" -> SHA256.(to_raw_string (hmac_string ~key data))
  | "sha384" -> SHA384.(to_raw_string (hmac_string ~key data))
  | "sha512" -> SHA512.(to_raw_string (hmac_string ~key data))
  | _ -> ""

let () =
  Callback.register "celld_js_random" (fun n -> !random_source n);
  Callback.register "celld_js_log" (fun level msg -> !logger level msg);
  Callback.register "celld_js_digest" digest;
  Callback.register "celld_js_hmac" hmac

type outcome = Done of string | Suspended of string

external call_out : t -> string -> string -> (outcome, string) result = "celld_js_call"
external resume_out : t -> string -> (outcome, string) result = "celld_js_resume"

let json_str s =
  let b = Buffer.create (String.length s + 2) in
  Buffer.add_char b '"';
  String.iter
    (function
      | '"' -> Buffer.add_string b "\\\""
      | '\\' -> Buffer.add_string b "\\\\"
      | c when Char.code c < 0x20 -> Buffer.add_string b (Printf.sprintf "\\u%04x" (Char.code c))
      | c -> Buffer.add_char b c)
    s;
  Buffer.add_char b '"';
  Buffer.contents b

let b64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

let base64 s =
  let b = Buffer.create ((String.length s + 2) / 3 * 4) and n = String.length s in
  let rec go i =
    if i < n then begin
      let c k = if i + k < n then Char.code s.[i + k] else 0 in
      let v = (c 0 lsl 16) lor (c 1 lsl 8) lor c 2 in
      Buffer.add_char b b64.[(v lsr 18) land 63];
      Buffer.add_char b b64.[(v lsr 12) land 63];
      Buffer.add_char b (if i + 1 < n then b64.[(v lsr 6) land 63] else '=');
      Buffer.add_char b (if i + 2 < n then b64.[v land 63] else '=');
      go (i + 3)
    end
  in
  go 0;
  Buffer.contents b

let unbase64 s =
  let v c = match c with 'A' .. 'Z' -> Char.code c - 65 | 'a' .. 'z' -> Char.code c - 71 | '0' .. '9' -> Char.code c + 4 | '+' -> 62 | '/' -> 63 | _ -> -1 in
  let b = Buffer.create (String.length s * 3 / 4) and acc = ref 0 and bits = ref 0 in
  String.iter
    (fun c ->
      let x = v c in
      if x >= 0 then begin
        acc := (!acc lsl 6) lor x;
        bits := !bits + 6;
        if !bits >= 8 then (bits := !bits - 8; Buffer.add_char b (Char.chr ((!acc lsr !bits) land 0xff)))
      end)
    s;
  Buffer.contents b

(* "status\nheaders-json\neffects\nbase64-body" *)
let parse_output out =
  match String.index_opt out '\n' with
  | None -> Error "malformed handler result"
  | Some a -> (
      match String.index_from_opt out (a + 1) '\n' with
      | None -> Error "malformed handler result"
      | Some b -> (
          match String.index_from_opt out (b + 1) '\n' with
          | None -> Error "malformed handler result"
          | Some c ->
              let status = Option.value (int_of_string_opt (String.sub out 0 a)) ~default:500 in
              let hjson = String.sub out (a + 1) (b - a - 1) and effects = String.sub out (b + 1) (c - b - 1) in
              let body = unbase64 (String.sub out (c + 1) (String.length out - c - 1)) in
              (* headers: a flat JSON object of strings, as the prelude writes it *)
              let headers =
                let rec fields i acc =
                  match String.index_from_opt hjson i '"' with
                  | None -> List.rev acc
                  | Some k0 -> (
                      let read_str j =
                        let buf = Buffer.create 16 in
                        let rec go j =
                          if j >= String.length hjson then (Buffer.contents buf, j)
                          else
                            match hjson.[j] with
                            | '"' -> (Buffer.contents buf, j + 1)
                            | '\\' when j + 1 < String.length hjson -> (
                                match hjson.[j + 1] with
                                | 'n' -> Buffer.add_char buf '\n'; go (j + 2)
                                | 'u' when j + 5 < String.length hjson -> Buffer.add_char buf (Char.chr (int_of_string ("0x" ^ String.sub hjson (j + 2) 4) land 0xff)); go (j + 6)
                                | ch -> Buffer.add_char buf ch; go (j + 2))
                            | ch -> Buffer.add_char buf ch; go (j + 1)
                        in
                        go j
                      in
                      let k, j = read_str (k0 + 1) in
                      match String.index_from_opt hjson j '"' with
                      | None -> List.rev acc
                      | Some v0 ->
                          let v, j = read_str (v0 + 1) in
                          fields j ((k, v) :: acc))
                in
                fields 0 []
              in
              Ok { status; headers; body; effects }))

let step_of = function
  | Error e -> Error e
  | Ok (Suspended ops) -> Ok (Await ops)
  | Ok (Done out) -> Result.map (fun r -> Reply r) (parse_output out)

let call t ~name ~cell ~meth ~path ~query ~body =
  let q = String.concat "," (List.map (fun (k, v) -> json_str k ^ ":" ^ json_str v) query) in
  let socks = String.concat "," (List.map json_str !sockets) in
  let hdrs = String.concat "," (List.map (fun (k, v) -> json_str (String.lowercase_ascii k) ^ ":" ^ json_str v) !headers) in
  let input =
    Printf.sprintf "{\"cell\":%s,\"method\":%s,\"path\":%s,\"query\":{%s},\"headers\":{%s},\"sockets\":[%s],\"body_b64\":%s}" (json_str cell)
      (json_str meth) (json_str path) q hdrs socks (json_str (base64 body))
  in
  step_of (call_out t name input)

let resume t results = step_of (resume_out t results)

(* For callers with nothing to await (tests, sync paths): a call that
   suspends is an error. *)
let call_sync t ~name ~cell ~meth ~path ~query ~body =
  match call t ~name ~cell ~meth ~path ~query ~body with
  | Ok (Reply r) -> Ok r
  | Ok (Await _) -> Error "this call needs I/O, which is not available here"
  | Error e -> Error e
