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
external call_raw : t -> string -> string -> (string, string) result = "celld_js_call"
external dispose : t -> unit = "celld_js_dispose"

(* After a call left promise jobs queued, a context only answers with an
   error: dispose it and create a new one from the script. *)
external poisoned : t -> bool = "celld_js_poisoned"

let prelude =
  {js|
"use strict";
globalThis.Response = class Response {
  constructor(body, init) {
    init = init || {};
    this.body = body == null ? "" : String(body);
    this.status = init.status || 200;
    this.webSocket = init.webSocket || null;
    this.headers = init.headers || {};
  }
  async text() { return this.body; }
  async json() { return JSON.parse(this.body); }
  static json(value, init) {
    init = init || {};
    const headers = Object.assign({ "content-type": "application/json" }, init.headers || {});
    return new Response(JSON.stringify(value), { status: init.status, headers });
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
const __celld_ctx = {
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
globalThis.__celld_dispatch = async (name, input) => {
  const out = await __celld_dispatch_inner(name, input);
  // status, content type, socket effects, body
  const nl = out.indexOf("\n"), nl2 = out.indexOf("\n", nl + 1);
  return out.slice(0, nl2 + 1) + __ws_ops() + "\n" + out.slice(nl2 + 1);
};
const __celld_dispatch_inner = async (name, input) => {
  const r = JSON.parse(input);
  __celld_ctx.id = r.cell;
  __celld_ws.open = new Set(r.sockets || []);
  __celld_ws.accepted = []; __celld_ws.sends = []; __celld_ws.closes = [];
  if (name === "ws_message" || name === "ws_close") {
    const ev = JSON.parse(r.body);
    const handlers = globalThis.default || globalThis;
    const ws = new __CelldWebSocket(ev.id);
    if (name === "ws_close") {
      __celld_ws.open.delete(ev.id);
      __ws_table();
      // The handler may still read the socket's attachment and tags.
      if (typeof handlers.webSocketClose === "function") await handlers.webSocketClose(ws, ev.code, ev.reason, ev.code !== 1006, __celld_ctx);
      __celld_sql_exec("DELETE FROM __celld_ws WHERE id = ?", [ev.id]);
    } else if (typeof handlers.webSocketMessage === "function") {
      await handlers.webSocketMessage(ws, ev.binary ? __from_b64(ev.data) : ev.data, __celld_ctx);
    }
    return "200\ntext/plain\n";
  }
  const handlers = globalThis.default || globalThis;
  const h = handlers[name];
  if (typeof h !== "function")
    return name === "alarm" ? "200\n\n" : "501\ntext/plain\nthis class has no " + name + " handler\n";
  if (name === "scheduled") {
    const input = JSON.parse(r.body);
    let noRetry = false;
    const controller = { cron: input.cron, scheduledTime: input.scheduledTime, noRetry() { noRetry = true; } };
    try {
      await h.call(handlers, controller, __celld_ctx);
      return "200\napplication/json\n{}";
    } catch (e) {
      return "500\napplication/json\n" + JSON.stringify({ error: String(e) + (e && e.stack ? "\n" + e.stack : ""), noRetry });
    }
  }
  if (name === "queue") {
    // A handler that throws has its writes rolled back and its whole batch
    // retried, acknowledged or not, so no message is lost with its effects.
    const input = JSON.parse(r.body);
    const settled = new Map();
    const messages = input.messages.map((m) => ({
      id: m.id,
      timestamp: new Date(m.timestamp),
      attempts: m.attempts,
      body: m.contentType === "text" ? m.body : JSON.parse(m.body),
      ack() { if (!settled.has(m.id)) settled.set(m.id, { ack: true }); },
      retry(o) { if (!settled.has(m.id)) settled.set(m.id, { ack: false, delay_ms: Math.round(((o && o.delaySeconds) || 0) * 1000) }); },
    }));
    const batch = {
      queue: input.queue,
      messages,
      ackAll() { messages.forEach((m) => m.ack()); },
      retryAll(o) { messages.forEach((m) => m.retry(o)); },
    };
    try {
      await h.call(handlers, batch, __celld_ctx);
    } catch (e) {
      return "500\napplication/json\n" + JSON.stringify({ error: String(e) + (e && e.stack ? "\n" + e.stack : "") });
    }
    const ack = [], retry = [];
    for (const m of input.messages) {
      const s = settled.get(m.id) || { ack: true };
      if (s.ack) ack.push(m.id); else retry.push({ id: m.id, delay_ms: s.delay_ms });
    }
    return "200\napplication/json\n" + JSON.stringify({ ack, retry });
  }
  let out;
  if (name === "alarm") out = await h.call(handlers, __celld_ctx);
  else {
    const qs = Object.entries(r.query).map(([k, v]) => encodeURIComponent(k) + "=" + encodeURIComponent(v)).join("&");
    const request = {
      method: r.method,
      url: "http://" + r.cell + r.path + (qs ? "?" + qs : ""),
      path: r.path,
      query: r.query,
      headers: r.headers || {},
      text: async () => r.body,
      json: async () => JSON.parse(r.body),
    };
    out = await h.call(handlers, request, __celld_ctx);
  }
  if (!(out instanceof Response)) out = typeof out === "string" ? new Response(out) : Response.json(out === undefined ? null : out);
  if (out.status === 101 && !__celld_ws.accepted.length) return "500\ntext/plain\na 101 response needs ctx.acceptWebSocket(server) first\n";
  const type = out.headers["content-type"] || out.headers["Content-Type"] || "text/plain";
  return out.status + "\n" + type + "\n" + out.body;
};
|js}

let create ~script ~db = create_raw prelude script db

type response = { status : int; content_type : string; body : string; effects : string  (** socket operations, JSON, or "" *) }

(* Open socket ids of the cell being called, set by the node before each
   call (calls are serial). *)
let sockets : string list ref = ref []
let headers : (string * string) list ref = ref []

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

let call t ~name ~cell ~meth ~path ~query ~body =
  let q = String.concat "," (List.map (fun (k, v) -> json_str k ^ ":" ^ json_str v) query) in
  let socks = String.concat "," (List.map json_str !sockets) in
  let hdrs = String.concat "," (List.map (fun (k, v) -> json_str (String.lowercase_ascii k) ^ ":" ^ json_str v) !headers) in
  let input =
    Printf.sprintf "{\"cell\":%s,\"method\":%s,\"path\":%s,\"query\":{%s},\"headers\":{%s},\"sockets\":[%s],\"body\":%s}" (json_str cell)
      (json_str meth) (json_str path) q hdrs socks (json_str body)
  in
  match call_raw t name input with
  | Error e -> Error e
  | Ok out -> (
      match String.split_on_char '\n' out with
      | status :: content_type :: effects :: _ ->
          let skip = String.length status + String.length content_type + String.length effects + 3 in
          let body = if skip <= String.length out then String.sub out skip (String.length out - skip) else "" in
          Ok { status = Option.value (int_of_string_opt status) ~default:500; content_type; body; effects }
      | _ -> Error "malformed handler result")
