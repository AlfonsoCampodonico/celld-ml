let check name c = if not c then (Printf.printf "FAIL %s\n" name; exit 1) else Printf.printf "ok   %s\n" name

let script =
  {|
function fetch(request, ctx) {
  const sql = ctx.storage.sql;
  sql.exec("CREATE TABLE IF NOT EXISTS hits(id INTEGER PRIMARY KEY, path TEXT)");
  if (request.method === "POST") sql.exec("INSERT INTO hits(path) VALUES (?)", request.path);
  if (request.path === "/boom") throw new Error("boom");
  if (request.path === "/spin") for (;;) {}
  if (request.path === "/kv") { ctx.storage.put("seen", (ctx.storage.get("seen") || 0) + 1); return String(ctx.storage.get("seen")); }
  return Response.json({ cell: ctx.id, hits: sql.exec("SELECT count(*) AS n FROM hits")[0].n });
}
async function alarm(ctx) { ctx.storage.put("alarmed", true); }
|}

let () =
  let db = Celld_sqlite.open_image "" in
  match Celld_js.create ~script ~db with
  | Error e -> check ("create: " ^ e) false
  | Ok js ->
      let call ?(meth = "GET") path = Celld_js.call js ~name:"fetch" ~cell:"Hits:a" ~meth ~path ~query:[] ~body:"" in
      (match call ~meth:"POST" "/x" with
      | Ok r -> check "fetch returns JSON" (r.status = 200 && r.content_type = "application/json" && r.body = {|{"cell":"Hits:a","hits":1}|})
      | Error e -> check ("fetch: " ^ e) false);
      check "storage writes dirty the database" (snd (Celld_sqlite.dirty db) <> []);
      (match call "/boom" with Error e -> check "exceptions surface" (String.length e >= 4 && String.sub e 0 11 = "Error: boom") | Ok _ -> check "exceptions surface" false);
      (match call "/spin" with Error e -> check ("runaway script interrupted: " ^ String.sub e 0 (min 30 (String.length e))) true | Ok _ -> check "runaway script interrupted" false);
      (match call "/kv" with Ok r -> check "kv storage" (r.body = "1") | Error e -> check e false);
      (match call "/kv" with Ok r -> check "kv storage persists" (r.body = "2") | Error e -> check e false);
      (match Celld_js.call js ~name:"alarm" ~cell:"Hits:a" ~meth:"" ~path:"" ~query:[] ~body:"" with
      | Ok _ -> check "async alarm" (match call "/kv" with Ok _ -> true | Error _ -> false)
      | Error e -> check e false);
      (match Celld_js.create ~script:"syntax error here (" ~db with Error _ -> check "bad script rejected" true | Ok _ -> check "bad script rejected" false);
      Celld_js.dispose js

(* Isolation and limits: each cell has its own runtime; deep recursion is a
   catchable error, not a crash; a cell's memory limit is its own. *)
let () =
  let db1 = Celld_sqlite.open_image "" and db2 = Celld_sqlite.open_image "" in
  let mk db script = match Celld_js.create ~script ~db with Ok t -> t | Error e -> failwith e in
  let a = mk db1 {|let n = 0; function fetch() { n += 1; return String(n); }|} in
  let b = mk db2 {|function fetch(r) {
    if (r.path === "/deep") { const f = (x) => f(x + 1); f(0); }
    if (r.path === "/hog") { const xs = []; for (;;) xs.push(new Array(100000).fill(1)); }
    if (r.path === "/json") return String(JSON.parse(r.query.s).length);
    return "b"; }|} in
  let call t ?(query = []) path = Celld_js.call t ~name:"fetch" ~cell:"X:1" ~meth:"GET" ~path ~query ~body:"" in
  check "cells keep separate globals" (call a "/" = Ok { status = 200; content_type = "text/plain"; body = "1" } && (match call b "/" with Ok r -> r.body = "b" | _ -> false));
  (match call b "/deep" with Error e -> check ("deep recursion is an error: " ^ String.sub e 0 (min 40 (String.length e))) true | Ok _ -> check "deep recursion" false);
  (match call b "/hog" with Error _ -> check "memory limit is per cell" true | Ok _ -> check "memory limit" false);
  check "other cell unaffected by a hog" (match call a "/" with Ok r -> r.body = "2" | _ -> false);
  let deep = String.make 100_000 '[' ^ String.make 100_000 ']' in
  (match call b ~query:[ ("s", deep) ] "/json" with Error _ -> check "deeply nested JSON is an error" true | Ok _ -> check "deeply nested JSON" true);
  check "cell usable after errors" (match call b "/" with Ok r -> r.body = "b" | _ -> false);
  Celld_js.dispose a;
  Celld_js.dispose b
