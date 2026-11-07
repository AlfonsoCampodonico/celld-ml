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

let prelude =
  {js|
"use strict";
globalThis.Response = class Response {
  constructor(body, init) {
    init = init || {};
    this.body = body == null ? "" : String(body);
    this.status = init.status || 200;
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
const __celld_kv = () => {
  __celld_sql_exec("CREATE TABLE IF NOT EXISTS __celld_kv(k TEXT PRIMARY KEY, v TEXT NOT NULL)");
};
const __celld_ctx = {
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
globalThis.__celld_dispatch = async (name, input) => {
  const r = JSON.parse(input);
  __celld_ctx.id = r.cell;
  const handlers = globalThis.default || globalThis;
  const h = handlers[name];
  if (typeof h !== "function")
    return name === "alarm" ? "200\n\n" : "501\ntext/plain\nthis class has no fetch handler\n";
  let out;
  if (name === "alarm") out = await h.call(handlers, __celld_ctx);
  else {
    const qs = Object.entries(r.query).map(([k, v]) => encodeURIComponent(k) + "=" + encodeURIComponent(v)).join("&");
    const request = {
      method: r.method,
      url: "http://" + r.cell + r.path + (qs ? "?" + qs : ""),
      path: r.path,
      query: r.query,
      headers: {},
      text: async () => r.body,
      json: async () => JSON.parse(r.body),
    };
    out = await h.call(handlers, request, __celld_ctx);
  }
  if (!(out instanceof Response)) out = typeof out === "string" ? new Response(out) : Response.json(out === undefined ? null : out);
  const type = out.headers["content-type"] || out.headers["Content-Type"] || "text/plain";
  return out.status + "\n" + type + "\n" + out.body;
};
|js}

let create ~script ~db = create_raw prelude script db

type response = { status : int; content_type : string; body : string }

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
  let input =
    Printf.sprintf "{\"cell\":%s,\"method\":%s,\"path\":%s,\"query\":{%s},\"body\":%s}" (json_str cell) (json_str meth) (json_str path) q (json_str body)
  in
  match call_raw t name input with
  | Error e -> Error e
  | Ok out -> (
      match String.split_on_char '\n' out with
      | status :: content_type :: _ ->
          let skip = String.length status + String.length content_type + 2 in
          let body = if skip <= String.length out then String.sub out skip (String.length out - skip) else "" in
          Ok { status = Option.value (int_of_string_opt status) ~default:500; content_type; body }
      | _ -> Error "malformed handler result")
