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
  if (__loop ? __celld_ops.waiting.size >= 200 : ++__celld_ops.count > 50) { reject(new Error(__loop ? "too many operations in flight (200)" : "a call makes at most 50 subrequests")); return; }
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
    this._chunks = []; this._closed = false; this._error = null; this.locked = false; this._waiters = [];
    const wake = () => { const w = this._waiters; this._waiters = []; w.forEach((f) => f()); };
    this._ctrl = {
      enqueue: (x) => { this._chunks.push(x); wake(); },
      close: () => { this._closed = true; wake(); },
      error: (e) => { this._error = e; wake(); },
      get desiredSize() { return 1; },
    };
    this._pull = src.pull ? () => src.pull(this._ctrl) : null;
    this._cancel = src.cancel || null;
    if (src.start) src.start(this._ctrl);
  }
  static __from(bytes) { return new ReadableStream({ start(c) { c.enqueue(bytes); c.close(); } }); }
  async __next() {
    for (;;) {
      if (this._chunks.length) return { value: this._chunks.shift(), done: false };
      if (this._error) throw this._error;
      if (this._closed) return { value: undefined, done: true };
      if (this._pull) await this._pull();
      else await new Promise((r) => this._waiters.push(r));
    }
  }
  getReader() {
    const s = this;
    s.locked = true;
    return {
      read: () => s.__next(),
      releaseLock() { s.locked = false; },
      async cancel(r) { await s.cancel(r); },
      get closed() { return new Promise((res) => { const check = () => (s._closed ? res() : s._waiters.push(check)); check(); }); },
    };
  }
  async cancel(r) { this._closed = true; this._chunks = []; if (this._cancel) await this._cancel(r); }
  async *[Symbol.asyncIterator]() { for (;;) { const x = await this.__next(); if (x.done) return; yield x.value; } }
  async pipeTo(ws, opts) {
    const w = ws.getWriter();
    try { for await (const c of this) await w.write(c); if (!(opts && opts.preventClose)) await w.close(); }
    catch (e) { await w.abort(e); throw e; }
    finally { w.releaseLock(); }
  }
  pipeThrough(t, opts) { this.pipeTo(t.writable, opts).catch(() => {}); return t.readable; }
  tee() {
    const a = [], b = [], src = this;
    let done = false, err = null, reading = null;
    const pump = async () => { const x = await src.__next(); if (x.done) done = true; else { a.push(x.value); b.push(x.value); } };
    const branch = (q) => new ReadableStream({
      async pull(c) {
        if (!q.length && !done && !err) { reading = reading || pump().catch((e) => { err = e; }).finally(() => { reading = null; }); await reading; }
        if (q.length) c.enqueue(q.shift()); else if (err) c.error(err); else if (done) c.close();
      },
    });
    return [branch(a), branch(b)];
  }
  async __bytes() {
    const parts = [];
    for await (const c of this) parts.push(__u8(c));
    const n = parts.reduce((x, p) => x + p.length, 0), out = new Uint8Array(n);
    let o = 0;
    for (const p of parts) { out.set(p, o); o += p.length; }
    return out;
  }
};
globalThis.WritableStream = class WritableStream {
  constructor(sink) {
    this._sink = sink || {}; this.locked = false; this._error = null;
    if (this._sink.start) this._sink.start({ error: (e) => { this._error = e; } });
  }
  getWriter() {
    const s = this;
    s.locked = true;
    return {
      async write(c) { if (s._error) throw s._error; if (s._sink.write) await s._sink.write(c, {}); },
      async close() { if (s._sink.close) await s._sink.close(); },
      async abort(r) { if (s._sink.abort) await s._sink.abort(r); },
      releaseLock() { s.locked = false; },
      get ready() { return Promise.resolve(); },
      get closed() { return Promise.resolve(); },
      get desiredSize() { return 1; },
    };
  }
  async close() { const w = this.getWriter(); await w.close(); w.releaseLock(); }
  async abort(r) { if (this._sink.abort) await this._sink.abort(r); }
};
globalThis.TransformStream = class TransformStream {
  constructor(t) {
    t = t || {};
    let ctrl;
    this.readable = new ReadableStream({ start(c) { ctrl = c; } });
    const tc = { enqueue: (x) => ctrl.enqueue(x), terminate: () => ctrl.close(), error: (e) => ctrl.error(e), get desiredSize() { return 1; } };
    if (t.start) t.start(tc);
    this.writable = new WritableStream({
      write: async (chunk) => { if (t.transform) await t.transform(chunk, tc); else tc.enqueue(chunk); },
      close: async () => { if (t.flush) await t.flush(tc); ctrl.close(); },
      abort: async (e) => ctrl.error(e),
    });
  }
};
globalThis.TextEncoderStream = class TextEncoderStream extends TransformStream {
  constructor() { const e = new TextEncoder(); super({ transform(c, ctl) { ctl.enqueue(e.encode(String(c))); } }); }
};
globalThis.TextDecoderStream = class TextDecoderStream extends TransformStream {
  constructor() { const d = new TextDecoder(); super({ transform(c, ctl) { ctl.enqueue(d.decode(c)); } }); }
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
class CryptoKey { constructor(algorithm, raw, usages, type) { this.type = type || "secret"; this.extractable = true; this.algorithm = algorithm; this.usages = usages || []; this._raw = raw; } }
const __alg = (a) => { const o = typeof a === "string" ? { name: a } : Object.assign({}, a); o.name = String(o.name).toUpperCase() === "HMAC" ? "HMAC" : String(o.name).toUpperCase(); if (o.name === "AES-GCM" || o.name === "ECDSA" || o.name === "ECDH") return o; return o; };
const __key_alg = (a, bytes) =>
  a.name === "HMAC" ? { name: "HMAC", hash: { name: typeof a.hash === "string" ? a.hash : a.hash.name }, length: bytes * 8 }
  : a.name === "AES-GCM" ? { name: "AES-GCM", length: bytes * 8 } : { name: a.name, namedCurve: a.namedCurve || "P-256" };
const __cat = (parts) => { const n = parts.reduce((x, p) => x + p.length, 0), out = new Uint8Array(n); let o = 0; for (const p of parts) { out.set(p, o); o += p.length; } return out; };
const __b64url = (u) => __to_b64(u).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const __unb64url = (s) => new Uint8Array(__from_b64(String(s).replace(/-/g, "+").replace(/_/g, "/")));
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
    // HMAC (any SHA), AES-GCM, ECDSA and ECDH on P-256. Keys: raw and JWK.
    async importKey(format, key, alg, extractable, usages) {
      const a = __alg(alg);
      if (format === "jwk") {
        if (a.name === "HMAC" || a.name === "AES-GCM") return new CryptoKey(__key_alg(a, __unb64url(key.k).length), __unb64url(key.k), usages, "secret");
        if (a.name === "ECDSA" || a.name === "ECDH") {
          const pub = __cat([new Uint8Array([4]), __unb64url(key.x), __unb64url(key.y)]);
          if (key.d) { const k = new CryptoKey(__key_alg(a), __unb64url(key.d), usages, "private"); k._pub = pub; return k; }
          return new CryptoKey(__key_alg(a), pub, usages, "public");
        }
      }
      if (format === "raw") {
        if (a.name === "HMAC" || a.name === "AES-GCM") return new CryptoKey(__key_alg(a, __u8(key).length), new Uint8Array(__u8(key)), usages, "secret");
        if (a.name === "ECDSA" || a.name === "ECDH") return new CryptoKey(__key_alg(a), new Uint8Array(__u8(key)), usages, "public");
      }
      throw new TypeError("unsupported key: " + format + " " + a.name);
    },
    async generateKey(alg, extractable, usages) {
      const a = __alg(alg);
      if (a.name === "HMAC") return new CryptoKey(__key_alg(a, (a.length || 256) / 8), new Uint8Array(__celld_random((a.length || 256) / 8)), usages, "secret");
      if (a.name === "AES-GCM") return new CryptoKey(__key_alg(a, (a.length || 256) / 8), new Uint8Array(__celld_random((a.length || 256) / 8)), usages, "secret");
      if (a.name === "ECDSA" || a.name === "ECDH") {
        if ((a.namedCurve || "P-256") !== "P-256") throw new TypeError("only P-256");
        const kp = new Uint8Array(__celld_crypto("p256-generate"));
        const privateKey = new CryptoKey(__key_alg(a), kp.slice(0, 32), usages, "private");
        privateKey._pub = kp.slice(32);
        return { privateKey, publicKey: new CryptoKey(__key_alg(a), kp.slice(32), usages, "public") };
      }
      throw new TypeError("unsupported algorithm " + a.name);
    },
    async exportKey(format, key) {
      if (format === "raw") { if (key.type === "private") throw new TypeError("private keys export as jwk"); return key._raw.buffer.slice(0); }
      if (format === "jwk") {
        if (key.type === "secret") return { kty: "oct", k: __b64url(key._raw), alg: key.algorithm.name === "AES-GCM" ? "A" + key._raw.length * 8 + "GCM" : undefined, ext: true };
        const pub = key.type === "private" ? key._pub : key._raw;
        const jwk = { kty: "EC", crv: "P-256", x: __b64url(pub.slice(1, 33)), y: __b64url(pub.slice(33, 65)), ext: true };
        if (key.type === "private") jwk.d = __b64url(key._raw);
        return jwk;
      }
      throw new TypeError("unsupported export format " + format);
    },
    async sign(alg, key, data) {
      const a = __alg(alg);
      if (a.name === "HMAC") return __celld_hmac(__hash_name(key.algorithm.hash), key._raw, __u8(data));
      if (a.name === "ECDSA") return __celld_crypto("p256-sign", key._raw, __celld_digest(__hash_name(a.hash), __u8(data)));
      throw new TypeError("unsupported algorithm " + a.name);
    },
    async verify(alg, key, sig, data) {
      const a = __alg(alg);
      if (a.name === "ECDSA") return new Uint8Array(__celld_crypto("p256-verify", key._raw, __celld_digest(__hash_name(a.hash), __u8(data)), __u8(sig)))[0] === 1;
      const want = new Uint8Array(__celld_hmac(__hash_name(key.algorithm.hash), key._raw, __u8(data))), got = __u8(sig);
      if (want.length !== got.length) return false;
      let d = 0;
      for (let i = 0; i < want.length; i++) d |= want[i] ^ got[i];
      return d === 0;
    },
    async encrypt(alg, key, data) {
      const a = __alg(alg);
      if (a.name !== "AES-GCM") throw new TypeError("unsupported algorithm " + a.name);
      return __celld_crypto("aes-gcm-encrypt", key._raw, __u8(a.iv), __u8(data), __u8(a.additionalData || new Uint8Array(0)));
    },
    async decrypt(alg, key, data) {
      const a = __alg(alg);
      if (a.name !== "AES-GCM") throw new TypeError("unsupported algorithm " + a.name);
      return __celld_crypto("aes-gcm-decrypt", key._raw, __u8(a.iv), __u8(data), __u8(a.additionalData || new Uint8Array(0)));
    },
    async deriveBits(alg, key, length) {
      const a = __alg(alg);
      if (a.name !== "ECDH") throw new TypeError("unsupported algorithm " + a.name);
      const bits = new Uint8Array(__celld_crypto("p256-ecdh", key._raw, a.public._raw));
      return bits.slice(0, (length || 256) / 8).buffer;
    },
    timingSafeEqual(a, b) { const x = __u8(a), y = __u8(b); if (x.length !== y.length) return false; let d = 0; for (let i = 0; i < x.length; i++) d |= x[i] ^ y[i]; return d === 0; },
  },
};

const __wait_until = [];
let __tx_depth = 0;
const __gate = { closed: 0 };
let __loop = false;
const __enqueue = async (queue, messages) => {
  if (typeof queue !== "string" || !/^[A-Za-z0-9_.$-]{1,200}$/.test(queue)) throw new TypeError("bad queue name");
  const payloads = messages.map((m) => {
    const opts = m.options || {}, type = opts.contentType || (typeof m.body === "string" ? "text" : "json");
    const body = type === "text" ? String(m.body) : JSON.stringify(m.body);
    if (body.length > 128000) throw new RangeError("a message is at most 128000 bytes");
    return { id: crypto.randomUUID().replace(/-/g, ""), body, content_type: type, delay_ms: Math.round((opts.delaySeconds || 0) * 1000) };
  });
  await __celld_op("enqueue", { queue, messages: payloads });
};
// ---- Cache API: a node-local cache ----
const __cache = (name) => ({
  async match(req) {
    const r = req instanceof Request ? req : new Request(String(req));
    if (r.method !== "GET") return undefined;
    const v = await __celld_op("cache", { op: "match", name, url: r.url });
    return v ? __stub_response(v) : undefined;
  },
  async put(req, res) {
    const r = req instanceof Request ? req : new Request(String(req));
    if (r.method !== "GET") throw new TypeError("only GET requests are cached");
    const cc = (res.headers.get("cache-control") || "").toLowerCase();
    if (/no-store|private/.test(cc) || res.status === 206) return;
    const m = cc.match(/s-maxage=(\d+)/) || cc.match(/max-age=(\d+)/);
    const ttl = m ? Number(m[1]) : 3600;
    const body = await res.bytes(); // put consumes the response, as in Workers
    await __celld_op("cache", { op: "put", name, url: r.url, ttl, status: res.status, headers: Object.fromEntries(res.headers), body: __to_b64(body) });
  },
  async delete(req) {
    const r = req instanceof Request ? req : new Request(String(req));
    return !!(await __celld_op("cache", { op: "delete", name, url: r.url }));
  },
});
globalThis.caches = { default: __cache("default"), async open(name) { return __cache(String(name)); } };
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
  // A cell's sends go out with its commit (the outbox); a stateless worker
  // has no commit, so its sends go straight to the queue.
  queue: (name) => ({
    send: async (body, options) => __celld_ctx.__stateless ? __enqueue(name, [{ body, options }]) : __celld_outbox(name, [{ body, options }]),
    sendBatch: async (messages) => {
      const ms = messages.map((m) => ({ body: m.body, options: m }));
      return __celld_ctx.__stateless ? __enqueue(name, ms) : __celld_outbox(name, ms);
    },
  }),
  // A stateless worker of another class, run on this node (service binding).
  service(name) {
    return {
      async fetch(input, init) {
        const req = input instanceof Request && !init ? input : new Request(String(input).startsWith("/") ? "http://service" + input : input, init);
        const u = new URL(req.url);
        const body = await req.bytes();
        return __stub_response(await __celld_op("service", { class: name, method: req.method, path: u.pathname + u.search, headers: Object.fromEntries(req.headers), body: __to_b64(body) }));
      },
    };
  },
  storage: {
    sql: { exec: (query, ...bindings) => __celld_sql_exec(query, bindings) },
    // Writes apply as they happen; these make a group of them atomic.
    transactionSync(fn) {
      const name = "celld_tx" + (__tx_depth++);
      __celld_sql_exec("SAVEPOINT " + name, []);
      try { const v = fn(); __celld_sql_exec("RELEASE " + name, []); return v; }
      catch (e) { __celld_sql_exec("ROLLBACK TO " + name, []); __celld_sql_exec("RELEASE " + name, []); throw e; }
      finally { __tx_depth--; }
    },
    // While an async transaction is open, no other event starts on the cell.
    async transaction(fn) {
      const name = "celld_tx" + (__tx_depth++);
      __gate.closed++;
      __celld_sql_exec("SAVEPOINT " + name, []);
      try { const v = await fn(__celld_ctx.storage); __celld_sql_exec("RELEASE " + name, []); return v; }
      catch (e) { __celld_sql_exec("ROLLBACK TO " + name, []); __celld_sql_exec("RELEASE " + name, []); throw e; }
      finally { __tx_depth--; __gate.closed--; }
    },
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
// In loop mode the node collects socket effects through __celld_poll.
const __out = (status, headers, body) => status + "\n" + JSON.stringify(headers) + "\n" + (__loop ? "" : __ws_ops()) + "\n" + __to_b64(__u8(body === undefined || body === null ? "" : body));
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
// Stateless workers have no storage.
const __storage = __celld_ctx.storage;
Object.defineProperty(__celld_ctx, "storage", { get() { if (__celld_ctx.__stateless) throw new Error("a stateless worker has no storage; use a cell"); return __storage; } });
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

// ---- the event loop (interleaved calls) ----
// The node starts calls, then polls: finished calls, queued operations,
// socket effects, and whether a transaction holds the input gate. Calls
// interleave whenever one awaits.
const __calls = { done: [] };
let __pending_wait = 0;
globalThis.__celld_start = (json) => {
  __loop = true;
  const { call, name, input } = JSON.parse(json);
  const r = JSON.parse(input);
  __celld_ctx.id = r.cell;
  __celld_ctx.__stateless = !!r.stateless;
  for (const id of r.sockets || []) __celld_ws.open.add(id);
  const before = __wait_until.length;
  __celld_dispatch_inner(name, r).then(
    (out) => __calls.done.push({ call, out }),
    (e) => __calls.done.push({ call, error: String(e) + (e && e.stack ? "\n" + e.stack : "") }));
  return "";
};
globalThis.__celld_poll = () => {
  // waitUntil work runs on after its response; count it as busy.
  while (__wait_until.length) { __pending_wait++; __wait_until.shift().finally(() => { __pending_wait--; }); }
  const done = __calls.done; __calls.done = [];
  const ops = __celld_ops.queued; __celld_ops.queued = [];
  const effects = __ws_ops();
  __celld_ws.accepted = []; __celld_ws.sends = []; __celld_ws.closes = [];
  return JSON.stringify({ done, ops, effects, gate: __gate.closed > 0, busy: __celld_ops.waiting.size > 0 || __pending_wait > 0 });
};
|js}


(* Node.js compatibility (a subset) and HTMLRewriter, loaded after the core
   prelude. *)
let prelude_more =
  {js|
"use strict";
// One function scope, so these names are not global lexical bindings: a
// class script may declare its own Buffer or EventEmitter (as Node code
// does with require).
(() => {
// ---- Node.js compatibility: Buffer, process, events, util, path, crypto ----
class Buffer extends Uint8Array {
  static from(v, enc) {
    if (typeof v === "string") {
      if (enc === "base64" || enc === "base64url") return Buffer.__wrap(new Uint8Array(__from_b64(enc === "base64url" ? v.replace(/-/g, "+").replace(/_/g, "/") : v)));
      if (enc === "hex") { const out = new Buffer(v.length >> 1); for (let i = 0; i < out.length; i++) out[i] = parseInt(v.substr(i * 2, 2), 16); return out; }
      if (enc === "latin1" || enc === "binary" || enc === "ascii") { const out = new Buffer(v.length); for (let i = 0; i < v.length; i++) out[i] = v.charCodeAt(i) & 255; return out; }
      return Buffer.__wrap(new TextEncoder().encode(v));
    }
    if (v instanceof ArrayBuffer) return Buffer.__wrap(new Uint8Array(v));
    if (ArrayBuffer.isView(v)) return Buffer.__wrap(new Uint8Array(v.buffer, v.byteOffset, v.byteLength));
    if (Array.isArray(v)) return Buffer.__wrap(Uint8Array.from(v));
    if (v && v.type === "Buffer" && Array.isArray(v.data)) return Buffer.__wrap(Uint8Array.from(v.data));
    throw new TypeError("Buffer.from: unsupported value");
  }
  static __wrap(u8) { const b = new Buffer(u8.length); b.set(u8); return b; }
  static alloc(n, fill) { const b = new Buffer(n); if (fill !== undefined) b.fill(typeof fill === "string" ? fill.charCodeAt(0) : fill); return b; }
  static allocUnsafe(n) { return new Buffer(n); }
  static isBuffer(x) { return x instanceof Buffer; }
  static isEncoding(e) { return ["utf8", "utf-8", "hex", "base64", "base64url", "latin1", "binary", "ascii"].includes(String(e).toLowerCase()); }
  static byteLength(s, enc) { return typeof s === "string" ? Buffer.from(s, enc).length : s.byteLength; }
  static concat(list, total) {
    total = total === undefined ? list.reduce((a, b) => a + b.length, 0) : total;
    const out = Buffer.alloc(total);
    let o = 0;
    for (const b of list) { if (o >= total) break; out.set(b.subarray(0, Math.min(b.length, total - o)), o); o += b.length; }
    return out;
  }
  static compare(a, b) { const n = Math.min(a.length, b.length); for (let i = 0; i < n; i++) if (a[i] !== b[i]) return a[i] < b[i] ? -1 : 1; return a.length === b.length ? 0 : a.length < b.length ? -1 : 1; }
  toString(enc, start, end) {
    const u = this.subarray(start || 0, end === undefined ? this.length : end);
    enc = String(enc || "utf8").toLowerCase();
    if (enc === "hex") return Array.from(u, (x) => x.toString(16).padStart(2, "0")).join("");
    if (enc === "base64") return __to_b64(u);
    if (enc === "base64url") return __to_b64(u).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
    if (enc === "latin1" || enc === "binary" || enc === "ascii") { let s = ""; for (const x of u) s += String.fromCharCode(x); return s; }
    return new TextDecoder().decode(u);
  }
  equals(o) { return Buffer.compare(this, o) === 0; }
  compare(o) { return Buffer.compare(this, o); }
  slice(a, b) { return this.subarray(a, b); }
  write(str, offset, enc) { offset = offset || 0; const b = Buffer.from(str, enc), n = Math.min(b.length, this.length - offset); this.set(b.subarray(0, n), offset); return n; }
  copy(target, ts, ss, se) { const src = this.subarray(ss || 0, se === undefined ? this.length : se); target.set(src.subarray(0, Math.min(src.length, target.length - (ts || 0))), ts || 0); return src.length; }
  readUInt8(o) { return this[o]; }
  readUInt16BE(o) { return (this[o] << 8) | this[o + 1]; }
  readUInt16LE(o) { return this[o] | (this[o + 1] << 8); }
  readUInt32BE(o) { return ((this[o] << 24) >>> 0) + (this[o + 1] << 16) + (this[o + 2] << 8) + this[o + 3]; }
  readUInt32LE(o) { return ((this[o + 3] << 24) >>> 0) + (this[o + 2] << 16) + (this[o + 1] << 8) + this[o]; }
  writeUInt8(v, o) { this[o] = v; return o + 1; }
  writeUInt16BE(v, o) { this[o] = v >> 8; this[o + 1] = v; return o + 2; }
  writeUInt32BE(v, o) { this[o] = v >>> 24; this[o + 1] = v >>> 16; this[o + 2] = v >>> 8; this[o + 3] = v; return o + 4; }
  toJSON() { return { type: "Buffer", data: Array.from(this) }; }
}
globalThis.Buffer = Buffer;
class EventEmitter {
  constructor() { this._ev = new Map(); }
  on(e, f) { if (!this._ev.has(e)) this._ev.set(e, []); this._ev.get(e).push(f); return this; }
  addListener(e, f) { return this.on(e, f); }
  prependListener(e, f) { if (!this._ev.has(e)) this._ev.set(e, []); this._ev.get(e).unshift(f); return this; }
  once(e, f) { const w = (...a) => { this.off(e, w); f.apply(this, a); }; w.listener = f; return this.on(e, w); }
  off(e, f) { const l = this._ev.get(e); if (l) { const i = l.findIndex((x) => x === f || x.listener === f); if (i >= 0) l.splice(i, 1); } return this; }
  removeListener(e, f) { return this.off(e, f); }
  removeAllListeners(e) { if (e === undefined) this._ev.clear(); else this._ev.delete(e); return this; }
  emit(e, ...a) { const l = this._ev.get(e); if (!l || !l.length) { if (e === "error") throw a[0]; return false; } for (const f of [...l]) f.apply(this, a); return true; }
  listenerCount(e) { return (this._ev.get(e) || []).length; }
  listeners(e) { return (this._ev.get(e) || []).map((f) => f.listener || f); }
  eventNames() { return [...this._ev.keys()]; }
  setMaxListeners() { return this; }
}
EventEmitter.EventEmitter = EventEmitter;
EventEmitter.default = EventEmitter;
EventEmitter.once = (em, e) => new Promise((res) => em.once(e, (...a) => res(a)));
globalThis.process = {
  env: {}, argv: [], platform: "celld", version: "v20.0.0", versions: { node: "20.0.0" }, pid: 1,
  nextTick: (f, ...a) => queueMicrotask(() => f(...a)),
  cwd: () => "/",
  hrtime: Object.assign((prev) => { const ns = Date.now() * 1e6; const t = [Math.floor(ns / 1e9), ns % 1e9]; return prev ? [t[0] - prev[0], t[1] - prev[1]] : t; }, { bigint: () => BigInt(Date.now()) * 1000000n }),
  memoryUsage: () => ({ rss: 0, heapTotal: 0, heapUsed: 0, external: 0 }),
  emitWarning: (w) => console.warn(String(w)),
  exit() { throw new Error("process.exit is not available in a cell"); },
};
const __path = {
  sep: "/",
  normalize(p) {
    const abs = p.startsWith("/"), out = [];
    for (const seg of p.split("/")) { if (seg === "" || seg === ".") continue; if (seg === "..") { if (out.length && out[out.length - 1] !== "..") out.pop(); else if (!abs) out.push(".."); } else out.push(seg); }
    const r = (abs ? "/" : "") + out.join("/");
    return r || (abs ? "/" : ".");
  },
  join: (...ps) => __path.normalize(ps.filter((x) => x !== "").join("/")),
  resolve: (...ps) => { let r = ""; for (const p of ps) r = p.startsWith("/") ? p : r + "/" + p; return __path.normalize(r.startsWith("/") ? r : "/" + r); },
  dirname: (p) => { const i = p.replace(/\/+$/, "").lastIndexOf("/"); return i < 0 ? "." : i === 0 ? "/" : p.slice(0, i); },
  basename: (p, ext) => { const b = p.replace(/\/+$/, "").split("/").pop(); return ext && b.endsWith(ext) ? b.slice(0, -ext.length) : b; },
  extname: (p) => { const b = __path.basename(p), i = b.lastIndexOf("."); return i <= 0 ? "" : b.slice(i); },
  isAbsolute: (p) => p.startsWith("/"),
  relative: (from, to) => { const a = __path.resolve(from).split("/").filter(Boolean), b = __path.resolve(to).split("/").filter(Boolean); let i = 0; while (i < a.length && a[i] === b[i]) i++; return [...a.slice(i).map(() => ".."), ...b.slice(i)].join("/"); },
};
__path.posix = __path;
const __hasher = (alg, key) => {
  const parts = [];
  const h = {
    update(d, enc) { parts.push(typeof d === "string" ? Buffer.from(d, enc) : __u8(d)); return h; },
    digest(enc) {
      const data = __cat(parts);
      const name = String(alg).toLowerCase().replace("-", "");
      const out = Buffer.from(key === undefined ? __celld_digest(name, data) : __celld_hmac(name, key, data));
      return enc ? out.toString(enc) : out;
    },
  };
  return h;
};
const __timing_safe = (a, b) => { if (a.length !== b.length) throw new RangeError("Input buffers must have the same byte length"); let d = 0; for (let i = 0; i < a.length; i++) d |= a[i] ^ b[i]; return d === 0; };
const __util = {
  promisify: (f) => (...a) => new Promise((res, rej) => f(...a, (e, v) => (e ? rej(e) : res(v)))),
  inspect: (v) => { try { return typeof v === "string" ? v : JSON.stringify(v); } catch (e) { return String(v); } },
  format: (f, ...a) => String(f).replace(/%[sdifjo%]/g, (m) => (m === "%%" ? "%" : a.length ? (m === "%j" || m === "%o" ? JSON.stringify(a.shift()) : String(a.shift())) : m)) + (a.length ? " " + a.map(String).join(" ") : ""),
  isDeepStrictEqual: (a, b) => JSON.stringify(a) === JSON.stringify(b),
  types: { isUint8Array: (x) => x instanceof Uint8Array, isPromise: (x) => x instanceof Promise },
  TextEncoder, TextDecoder,
};
const __assert = Object.assign((v, m) => { if (!v) throw new Error(m || "assertion failed"); }, {
  ok: (v, m) => { if (!v) throw new Error(m || "assertion failed"); },
  equal: (a, b, m) => { if (a != b) throw new Error(m || a + " == " + b); },
  strictEqual: (a, b, m) => { if (a !== b) throw new Error(m || a + " === " + b); },
  deepStrictEqual: (a, b, m) => { if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(m || "not deep-equal"); },
  throws: (f, m) => { try { f(); } catch (e) { return; } throw new Error(m || "did not throw"); },
});
const __modules = {
  buffer: { Buffer },
  events: EventEmitter,
  process: globalThis.process,
  path: __path,
  util: __util,
  assert: __assert,
  url: { URL, URLSearchParams },
  crypto: {
    randomUUID: () => crypto.randomUUID(),
    randomBytes: (n) => Buffer.from(__celld_random(n)),
    getRandomValues: (a) => crypto.getRandomValues(a),
    createHash: (alg) => __hasher(alg),
    createHmac: (alg, key) => __hasher(alg, typeof key === "string" ? new TextEncoder().encode(key) : __u8(key)),
    timingSafeEqual: __timing_safe,
    webcrypto: crypto,
    subtle: crypto.subtle,
  },
  timers: { setTimeout, clearTimeout, setInterval, clearInterval },
  stream: { ReadableStream, WritableStream, TransformStream },
  "stream/web": { ReadableStream, WritableStream, TransformStream, TextEncoderStream, TextDecoderStream },
  string_decoder: { StringDecoder: class { constructor() { this._d = new TextDecoder(); } write(b) { return this._d.decode(b); } end(b) { return b ? this._d.decode(b) : ""; } } },
};
globalThis.require = (name) => {
  const m = __modules[String(name).replace(/^node:/, "")];
  if (!m) throw new Error("Cannot find module '" + name + "' (celld-ml has a Node.js subset: " + Object.keys(__modules).join(", ") + ")");
  return m;
};

// ---- HTMLRewriter ----
// Cloudflare's API over a non-streaming tokenizer: the whole body is
// rewritten at once. Selectors: tag, *, .class, #id, [attr], [attr=v],
// [attr^=v], [attr$=v], [attr*=v], [attr~=v], compounds, and descendant
// (space) and child (>) combinators.
const __VOID = new Set(["area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"]);
const __RAW = new Set(["script", "style", "textarea", "title"]);
const __esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const __esc_attr = (s) => String(s).replace(/&/g, "&amp;").replace(/"/g, "&quot;");
const __parse_selector = (sel) => {
  const steps = [];
  let comb = " ";
  const re = /\s*(>)\s*|\s+|([^\s>]+)/g;
  let m;
  while ((m = re.exec(sel.trim()))) {
    if (m[1]) { comb = ">"; continue; }
    if (!m[2]) { if (steps.length) comb = comb === ">" ? ">" : " "; continue; }
    const c = { tag: null, id: null, classes: [], attrs: [] };
    const part = /([a-zA-Z][\w-]*|\*)|#([\w-]+)|\.([\w-]+)|\[\s*([\w-:]+)\s*(?:([~^$*]?=)\s*(?:"([^"]*)"|'([^']*)'|([^\]\s]*)))?\s*\]/g;
    let p;
    while ((p = part.exec(m[2]))) {
      if (p[1]) c.tag = p[1] === "*" ? null : p[1].toLowerCase();
      else if (p[2]) c.id = p[2];
      else if (p[3]) c.classes.push(p[3]);
      else if (p[4]) c.attrs.push({ name: p[4].toLowerCase(), op: p[5], value: p[6] !== undefined ? p[6] : p[7] !== undefined ? p[7] : p[8] });
    }
    steps.push({ comb: steps.length ? comb : null, c });
    comb = " ";
  }
  return steps;
};
const __match_compound = (c, el) => {
  if (c.tag && c.tag !== el.tagName) return false;
  if (c.id && el.getAttribute("id") !== c.id) return false;
  const cls = (el.getAttribute("class") || "").split(/\s+/);
  for (const k of c.classes) if (!cls.includes(k)) return false;
  for (const a of c.attrs) {
    const v = el.getAttribute(a.name);
    if (v === null) return false;
    if (!a.op) continue;
    if (a.op === "=" && v !== a.value) return false;
    if (a.op === "^=" && !v.startsWith(a.value)) return false;
    if (a.op === "$=" && !v.endsWith(a.value)) return false;
    if (a.op === "*=" && !v.includes(a.value)) return false;
    if (a.op === "~=" && !v.split(/\s+/).includes(a.value)) return false;
  }
  return true;
};
// [stack] is the open elements, innermost last; the candidate is its top.
const __matches = (steps, stack) => {
  const go = (si, depth) => {
    if (si < 0) return true;
    const step = steps[si];
    if (si === steps.length - 1) return __match_compound(step.c, stack[depth]) && go(si - 1, depth - 1);
    const next = steps[si + 1].comb;
    if (next === ">") return depth >= 0 && __match_compound(step.c, stack[depth]) && go(si - 1, depth - 1);
    for (let d = depth; d >= 0; d--) if (__match_compound(step.c, stack[d]) && go(si - 1, d - 1)) return true;
    return false;
  };
  return go(steps.length - 1, stack.length - 1);
};
class __Content {
  constructor() { this.removed = false; this.replacement = null; }
  _c(x, o) { return o && o.html ? String(x) : __esc(x); }
}
class __Element extends __Content {
  constructor(tag, attrs, selfClosing) {
    super();
    this.tagName = tag; this._attrs = attrs; this.selfClosing = selfClosing;
    this.prepends = []; this.appends = []; this.inner = null; this.keepContent = false; this.endTagHandlers = []; this.removed = false;
  }
  get attributes() { return this._attrs[Symbol.iterator](); }
  getAttribute(n) { const a = this._attrs.find((x) => x[0] === n.toLowerCase()); return a ? a[1] : null; }
  hasAttribute(n) { return this.getAttribute(n) !== null; }
  setAttribute(n, v) { const a = this._attrs.find((x) => x[0] === n.toLowerCase()); if (a) a[1] = String(v); else this._attrs.push([n.toLowerCase(), String(v)]); return this; }
  removeAttribute(n) { this._attrs = this._attrs.filter((x) => x[0] !== n.toLowerCase()); return this; }
  before(x, o) { this.before_.push(this._c(x, o)); return this; }
  after(x, o) { this.after_.push(this._c(x, o)); return this; }
  prepend(x, o) { this.prepends.push(this._c(x, o)); return this; }
  append(x, o) { this.appends.push(this._c(x, o)); return this; }
  setInnerContent(x, o) { this.inner = this._c(x, o); return this; }
  replace(x, o) { this.replacement = this._c(x, o); this.removed = true; return this; }
  remove() { this.removed = true; return this; }
  removeAndKeepContent() { this.keepContent = true; return this; }
  onEndTag(f) { this.endTagHandlers.push(f); }
  get removed_() { return this.removed; }
}
Object.defineProperty(__Element.prototype, "before_", { get() { return this._before || (this._before = []); } });
Object.defineProperty(__Element.prototype, "after_", { get() { return this._after || (this._after = []); } });
class __Text extends __Content {
  constructor(text, last) { super(); this.text = text; this.lastInTextNode = last; this._b = []; this._a = []; }
  before(x, o) { this._b.push(this._c(x, o)); return this; }
  after(x, o) { this._a.push(this._c(x, o)); return this; }
  replace(x, o) { this.replacement = this._c(x, o); return this; }
  remove() { this.removed = true; return this; }
}
globalThis.HTMLRewriter = class HTMLRewriter {
  constructor() { this._on = []; this._doc = []; }
  on(selector, handlers) { this._on.push({ steps: __parse_selector(selector), h: handlers }); return this; }
  onDocument(handlers) { this._doc.push(handlers); return this; }
  transform(response) {
    const self = this;
    const out = (async () => self._run(await response.text()))();
    const headers = new Headers(response.headers);
    headers.delete("content-length");
    return new Response(new ReadableStream({ async pull(c) { c.enqueue(new TextEncoder().encode(await out)); c.close(); } }), { status: response.status, headers });
  }
  async _run(html) {
    const out = [];
    const stack = []; // { el, active (handlers that matched), skip }
    let suppress = 0; // > 0 while inside an element whose content is replaced or removed
    const emit = (s) => { if (!suppress) out.push(s); };
    const textHandlers = () => {
      const hs = this._doc.filter((d) => d.text).map((d) => d.text.bind(d));
      for (const f of stack) for (const m of f.matched) if (m.h.text) hs.push(m.h.text.bind(m.h));
      return hs;
    };
    const runText = async (txt, last) => {
      const hs = textHandlers();
      if (!hs.length) { emit(txt); return; }
      const t = new __Text(txt, last);
      for (const h of hs) await h(t);
      emit(t._b.join("") + (t.removed ? "" : t.replacement !== null ? t.replacement : txt) + t._a.join(""));
    };
    const re = /<!--[\s\S]*?-->|<!doctype[^>]*>|<\/\s*([a-zA-Z][\w:-]*)\s*>|<([a-zA-Z][\w:-]*)((?:\s+[^\s"'>\/=]+(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s"'>]+))?)*)\s*(\/?)>/gi;
    let pos = 0, m;
    while ((m = re.exec(html))) {
      if (m.index > pos) await runText(html.slice(pos, m.index), true);
      pos = re.lastIndex;
      const tok = m[0];
      if (tok.startsWith("<!--")) {
        const hs = [...this._doc.filter((d) => d.comments)];
        for (const f of stack) for (const mm of f.matched) if (mm.h.comments) hs.push(mm.h);
        if (!hs.length) { emit(tok); continue; }
        const c = new __Text(tok.slice(4, -3), true);
        c.text = tok.slice(4, -3);
        for (const h of hs) await h.comments(c);
        emit(c._b.join("") + (c.removed ? "" : c.replacement !== null ? c.replacement : "<!--" + c.text + "-->") + c._a.join(""));
        continue;
      }
      if (/^<!doctype/i.test(tok)) {
        for (const d of this._doc) if (d.doctype) await d.doctype({ name: "html", publicId: null, systemId: null });
        emit(tok);
        continue;
      }
      if (m[1]) {
        // end tag: close up to the matching open element
        const tag = m[1].toLowerCase();
        let i = stack.length - 1;
        while (i >= 0 && stack[i].el.tagName !== tag) i--;
        if (i < 0) { emit(tok); continue; }
        while (stack.length > i) {
          const f = stack.pop();
          if (f.suppressing) suppress--;
          const el = f.el;
          if (!(el.removed && !el.keepContent)) emit(el.appends.join(""));
          let endTag = "</" + el.tagName + ">";
          for (const h of el.endTagHandlers) { const et = { name: el.tagName, before: [], after: [], removed: false }; et.before = (x, o) => { et.b = (et.b || "") + (o && o.html ? x : __esc(x)); }; et.after = (x, o) => { et.a = (et.a || "") + (o && o.html ? x : __esc(x)); }; et.remove = () => { et.removed = true; }; await h(et); endTag = (et.b || "") + (et.removed ? "" : endTag) + (et.a || ""); }
          if (!el.removed && !el.keepContent) emit(endTag);
          else if (el.keepContent && !el.removed) {}
          emit(el.after_.join(""));
        }
        continue;
      }
      // start tag
      const tag = m[2].toLowerCase();
      const attrs = [];
      const ar = /([^\s"'>\/=]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'>]+)))?/g;
      let a;
      while ((a = ar.exec(m[3]))) attrs.push([a[1].toLowerCase(), a[2] !== undefined ? a[2] : a[3] !== undefined ? a[3] : a[4] !== undefined ? a[4] : ""]);
      const el = new __Element(tag, attrs, m[4] === "/" || __VOID.has(tag));
      const frame = { el, matched: [], suppressing: false };
      stack.push(frame);
      for (const on of this._on) if (__matches(on.steps, stack.map((f) => f.el))) frame.matched.push(on);
      for (const on of frame.matched) if (on.h.element) await on.h.element(el);
      emit(el.before_.join(""));
      const startTag = "<" + el.tagName + el._attrs.map(([k, v]) => " " + k + (v === "" ? "" : '="' + __esc_attr(v) + '"')).join("") + (m[4] === "/" ? " />" : ">");
      if (el.replacement !== null) emit(el.replacement);
      else if (!el.removed && !el.keepContent) emit(startTag);
      if (!el.removed || el.keepContent) emit(el.prepends.join(""));
      if (el.selfClosing) {
        stack.pop();
        emit(el.after_.join(""));
        continue;
      }
      if (el.inner !== null && !el.removed) { emit(el.inner); frame.suppressing = true; suppress++; }
      else if (el.removed && !el.keepContent) { frame.suppressing = true; suppress++; }
      if (__RAW.has(tag)) {
        // raw text up to the matching end tag
        const end = html.toLowerCase().indexOf("</" + tag, pos);
        const stop = end < 0 ? html.length : end;
        if (stop > pos) await runText(html.slice(pos, stop), true);
        pos = stop;
        re.lastIndex = stop;
      }
    }
    if (pos < html.length) await runText(html.slice(pos), true);
    while (stack.length) { const f = stack.pop(); if (f.suppressing) suppress--; emit(f.el.appends.join("")); }
    let doc = out.join("");
    for (const d of this._doc) if (d.end) { const e = { append: (x, o) => { doc += o && o.html ? x : __esc(x); } }; await d.end(e); }
    return doc;
  }
};

// ---- WebAssembly (wasm3, an interpreter) ----
// Numeric imports and exports (i32, i64 as BigInt, f32, f64) and an exported
// memory whose buffer is a live view; the view is detached when the memory
// moves, so take memory.buffer again after calling into the module.
class CompileError extends Error { constructor(m) { super(m); this.name = "CompileError"; } }
class LinkError extends Error { constructor(m) { super(m); this.name = "LinkError"; } }
class RuntimeError extends Error { constructor(m) { super(m); this.name = "RuntimeError"; } }
class Module {
  constructor(bytes) {
    this._bytes = new Uint8Array(__u8(bytes));
    if (!__celld_wasm_validate(this._bytes)) throw new CompileError("invalid WebAssembly module");
  }
}
class Instance {
  constructor(module, imports) {
    if (!(module instanceof Module)) module = new Module(module);
    let r;
    try {
      r = __celld_wasm_instantiate(module._bytes, (m, n) => { const v = imports && imports[m] && imports[m][n]; return typeof v === "function" ? v : undefined; });
    } catch (e) { throw new LinkError(e.message); }
    const id = r.id, exports = {};
    for (const [name] of r.exports)
      exports[name] = (...args) => {
        try { return __celld_wasm_call(id, name, args); }
        catch (e) { if (e instanceof TypeError && /^RuntimeError: /.test(e.message)) throw new RuntimeError(e.message.slice(14)); throw e; }
      };
    if (r.memory) exports.memory = { get buffer() { return __celld_wasm_memory(id); }, grow(pages) { return __celld_wasm_grow(id, pages); } };
    this.exports = Object.freeze(exports);
  }
}
globalThis.WebAssembly = {
  Module, Instance, CompileError, LinkError, RuntimeError,
  validate: (bytes) => __celld_wasm_validate(new Uint8Array(__u8(bytes))),
  compile: async (bytes) => new Module(bytes),
  async instantiate(src, imports) {
    if (src instanceof Module) return new Instance(src, imports);
    const module = new Module(src);
    return { module, instance: new Instance(module, imports) };
  },
};
})();
|js}

(* After a class script loads, its own fetch handler is set aside so the
   global fetch stays the outbound one. *)
let create ~script ~db = create_raw (prelude ^ prelude_more) (script ^ "\n;__celld_after_load();\n") db

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

(* AES-GCM and P-256 (ECDSA, ECDH) for Web Crypto: "O" ^ result, or
   "E" ^ message. *)
let crypto op args =
  let ok s = "O" ^ s and err s = "E" ^ s in
  try
    match op, args with
    | "aes-gcm-encrypt", [ key; iv; data; aad ] ->
        if not (List.mem (String.length key) [ 16; 24; 32 ]) then err "AES key must be 128, 192 or 256 bits"
        else if String.length iv = 0 then err "AES-GCM needs an iv"
        else
          let key = Mirage_crypto.AES.GCM.of_secret key in
          ok (Mirage_crypto.AES.GCM.authenticate_encrypt ~key ~nonce:iv ~adata:aad data)
    | "aes-gcm-decrypt", [ key; iv; data; aad ] -> (
        let key = Mirage_crypto.AES.GCM.of_secret key in
        match Mirage_crypto.AES.GCM.authenticate_decrypt ~key ~nonce:iv ~adata:aad data with
        | Some plain -> ok plain
        | None -> err "decryption failed")
    | "p256-generate", [] ->
        let priv, pub = Mirage_crypto_ec.P256.Dsa.generate () in
        ok (Mirage_crypto_ec.P256.Dsa.priv_to_octets priv ^ Mirage_crypto_ec.P256.Dsa.pub_to_octets pub)
    | "p256-sign", [ priv; digest ] -> (
        match Mirage_crypto_ec.P256.Dsa.priv_of_octets priv with
        | Error _ -> err "bad private key"
        | Ok key ->
            let r, s = Mirage_crypto_ec.P256.Dsa.sign ~key digest in
            ok (r ^ s))
    | "p256-verify", [ pub; digest; signature ] -> (
        match Mirage_crypto_ec.P256.Dsa.pub_of_octets pub with
        | Error _ -> err "bad public key"
        | Ok key ->
            let valid = String.length signature = 64 && Mirage_crypto_ec.P256.Dsa.verify ~key (String.sub signature 0 32, String.sub signature 32 32) digest in
            ok (if valid then "\001" else "\000"))
    | "p256-ecdh", [ priv; pub ] -> (
        match Mirage_crypto_ec.P256.Dh.secret_of_octets priv with
        | Error _ -> err "bad private key"
        | Ok (secret, _) -> (
            match Mirage_crypto_ec.P256.Dh.key_exchange secret pub with Ok shared -> ok shared | Error _ -> err "bad public key"))
    | _ -> err ("unsupported crypto operation " ^ op)
  with e -> err (Printexc.to_string e)

let () =
  Callback.register "celld_js_crypto" crypto;
  Callback.register "celld_js_random" (fun n -> !random_source n);
  Callback.register "celld_js_log" (fun level msg -> !logger level msg);
  Callback.register "celld_js_digest" digest;
  Callback.register "celld_js_hmac" hmac

type outcome = Done of string | Suspended of string

external call_out : t -> string -> string -> (outcome, string) result = "celld_js_call"
external resume_out : t -> string -> (outcome, string) result = "celld_js_resume"
external invoke : t -> string -> string -> (string, string) result = "celld_js_invoke"

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

(* The JSON a call receives. *)
let input_json ?(stateless = false) ~cell ~meth ~path ~query ~headers ~sockets ~body () =
  let q = String.concat "," (List.map (fun (k, v) -> json_str k ^ ":" ^ json_str v) query) in
  let socks = String.concat "," (List.map json_str sockets) in
  let hdrs = String.concat "," (List.map (fun (k, v) -> json_str (String.lowercase_ascii k) ^ ":" ^ json_str v) headers) in
  Printf.sprintf "{\"cell\":%s,\"method\":%s,\"path\":%s,\"query\":{%s},\"headers\":{%s},\"sockets\":[%s],\"stateless\":%b,\"body_b64\":%s}"
    (json_str cell) (json_str meth) (json_str path) q hdrs socks stateless (json_str (base64 body))

let call t ~name ~cell ~meth ~path ~query ~body =
  let input = input_json ~cell ~meth ~path ~query ~headers:!headers ~sockets:!sockets ~body () in
  step_of (call_out t name input)

let resume t results = step_of (resume_out t results)

(* For callers with nothing to await (tests, sync paths): a call that
   suspends is an error. *)
let call_sync t ~name ~cell ~meth ~path ~query ~body =
  match call t ~name ~cell ~meth ~path ~query ~body with
  | Ok (Reply r) -> Ok r
  | Ok (Await _) -> Error "this call needs I/O, which is not available here"
  | Error e -> Error e
