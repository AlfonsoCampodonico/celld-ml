(* Native cell classes, standing in for Durable Object classes. A handler
   mutates the store's map and says whether it wrote. The runtime commits
   the write, which yields the output gate's position. *)

module M = Store.M

type reply = { status : int; body : string; wrote : bool; content_type : string }

let ok ?(wrote = false) body = { status = 200; body; wrote; content_type = "text/plain" }
let err status body = { status; body; wrote = false; content_type = "text/plain" }

let int_of k kv = match M.find_opt k kv with Some v -> int_of_string_opt v |> Option.value ~default:0 | None -> 0

(* Counter: GET reads, POST /increment[?by=n] adds, DELETE resets. *)
let counter (s : Store.t) ~meth ~path ~query ~body:_ =
  match meth, path with
  | "GET", ("" | "/") -> ok (string_of_int (int_of "value" s.kv) ^ "\n")
  | "POST", "/increment" ->
      let by = match List.assoc_opt "by" query with Some b -> int_of_string_opt b |> Option.value ~default:1 | None -> 1 in
      let v = int_of "value" s.kv + by in
      s.kv <- M.add "value" (string_of_int v) s.kv;
      ok ~wrote:true (string_of_int v ^ "\n")
  | "DELETE", ("" | "/") ->
      s.kv <- M.remove "value" s.kv;
      ok ~wrote:true "0\n"
  | _ -> err 404 "counter: GET /, POST /increment, DELETE /\n"

(* Keys starting with NUL belong to the runtime (the alarm). *)
let reserved k = String.length k > 0 && k.[0] = '\000'

(* Kv: GET / lists, GET|PUT|DELETE /<key>. *)
let kv (s : Store.t) ~meth ~path ~query:_ ~body =
  let key = if String.length path > 1 then String.sub path 1 (String.length path - 1) else "" in
  match meth, key with
  | _, k when reserved k -> err 400 "reserved key\n"
  | "GET", "" -> ok (String.concat "" (List.filter_map (fun (k, _) -> if reserved k then None else Some (k ^ "\n")) (M.bindings s.kv)))
  | "GET", k -> ( match M.find_opt k s.kv with Some v -> ok v | None -> err 404 "not found\n")
  | "PUT", k when k <> "" ->
      s.kv <- M.add k body s.kv;
      ok ~wrote:true "ok\n"
  | "DELETE", k when k <> "" ->
      if M.mem k s.kv then begin
        s.kv <- M.remove k s.kv;
        ok ~wrote:true "deleted\n"
      end
      else err 404 "not found\n"
  | _ -> err 405 "kv: GET /, GET|PUT|DELETE /<key>\n"

(* ---- alarms ----
   One alarm per cell, stored in the cell's own state, so it is captured,
   replicated and restored like any write and moves with the cell. Firing
   is a write too: [ALARM /__alarm/fire] clears a due alarm and runs the
   class's alarm handler, and is a no-op when nothing is due, so a duplicate
   fire from a stale timer or a second node is harmless. *)

let alarm_of (s : Store.t) = Option.bind (Store.meta_get s "alarm") int_of_string_opt

let bump k (s : Store.t) = s.kv <- M.add k (string_of_int (int_of k s.kv + 1)) s.kv

(* Counter's alarm ticks the counter; Kv's counts its firings. *)
let counter_alarm s = bump "value" s; bump "alarms" s
let kv_alarm s = bump "alarms_fired" s

type handler =
  Store.t -> now_ms:int -> meth:string -> path:string -> query:(string * string) list -> body:string -> reply

let with_alarm on_alarm h : handler =
 fun s ~now_ms ~meth ~path ~query ~body ->
  match meth, path with
  | "GET", "/__alarm" -> ok ((match alarm_of s with Some a -> string_of_int a | None -> "none") ^ "\n")
  | "PUT", "/__alarm" -> (
      let at =
        match List.assoc_opt "in_ms" query, List.assoc_opt "at_ms" query with
        | Some n, _ -> Option.map (fun n -> now_ms + n) (int_of_string_opt n)
        | None, Some a -> int_of_string_opt a
        | None, None -> int_of_string_opt (String.trim body)
      in
      match at with
      | Some at ->
          Store.meta_set s "alarm" (string_of_int at);
          ok ~wrote:true (string_of_int at ^ "\n")
      | None -> err 400 "PUT /__alarm?in_ms=N or ?at_ms=T\n")
  | "DELETE", "/__alarm" ->
      if alarm_of s = None then ok "none\n"
      else begin
        Store.meta_remove s "alarm";
        ok ~wrote:true "cleared\n"
      end
  | "ALARM", "/__alarm/fire" -> (
      match alarm_of s with
      | Some at when at <= now_ms ->
          Store.meta_remove s "alarm";
          on_alarm s;
          ok ~wrote:true "fired\n"
      | _ -> ok "not due\n")
  | _ -> h s ~meth ~path ~query ~body

(* ---- Sql: a SQLite database per cell ----
   POST /query runs the body as SQL and answers the last result set as
   JSON. GET / lists the tables. A request must leave no transaction open:
   one that does is rolled back, so every commit is one request. *)

let json_str s =
  let b = Buffer.create (String.length s + 2) in
  Buffer.add_char b '"';
  String.iter
    (function
      | '"' -> Buffer.add_string b "\\\""
      | '\\' -> Buffer.add_string b "\\\\"
      | '\n' -> Buffer.add_string b "\\n"
      | c when Char.code c < 0x20 -> Buffer.add_string b (Printf.sprintf "\\u%04x" (Char.code c))
      | c -> Buffer.add_char b c)
    s;
  Buffer.add_char b '"';
  Buffer.contents b

let json_result (r : Celld_sqlite.result_set) =
  let row cells = "[" ^ String.concat "," (List.map (function None -> "null" | Some v -> json_str v) cells) ^ "]" in
  Printf.sprintf "{\"columns\":%s,\"rows\":[%s]}\n" (row (List.map Option.some r.columns)) (String.concat "," (List.map row r.rows))

let sql (s : Store.t) ~meth ~path ~query:_ ~body =
  match s.sql with
  | None -> err 500 "not a SQL cell\n"
  | Some db -> (
      let run q =
        let r = Celld_sqlite.exec db q in
        if not (Celld_sqlite.autocommit db) then begin
          ignore (Celld_sqlite.exec db "ROLLBACK");
          err 400 "transaction left open; rolled back\n"
        end
        else
          match r with
          | Error e -> err 400 (e ^ "\n")
          | Ok rs -> { status = 200; body = json_result rs; wrote = Store.changed s; content_type = "application/json" }
      in
      match meth, path with
      | "POST", "/query" -> run body
      | "GET", ("" | "/") ->
          run (Printf.sprintf "SELECT name FROM sqlite_master WHERE type = 'table' AND name <> '%s' ORDER BY name" Store.meta_table)
      | _ -> err 404 "sql: GET /, POST /query\n")

let sql_alarm (s : Store.t) =
  Option.iter
    (fun db -> ignore (Celld_sqlite.exec db "CREATE TABLE IF NOT EXISTS alarms_fired(at INTEGER); INSERT INTO alarms_fired VALUES (unixepoch('subsec') * 1000)"))
    s.sql

let class_of cell = match String.index_opt cell ':' with Some i -> String.sub cell 0 i | None -> "Counter"

let valid_cell id =
  let n = String.length id in
  n > 0 && n <= 255 && id <> "." && id <> ".."
  && String.for_all (function 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '.' | ':' | '$' | '-' -> true | _ -> false) id

let dispatch cell : handler option =
  match class_of cell with
  | "Counter" -> Some (with_alarm counter_alarm counter)
  | "Kv" -> Some (with_alarm kv_alarm kv)
  | "Sql" -> Some (with_alarm sql_alarm sql)
  | _ -> None

(* ---- JS classes ----
   Any other class name is a JS class whose script lives in the bucket at
   classes/<Class>.js. Its storage is the cell's SQLite database, and each
   call runs inside a savepoint: a handler that throws leaves no writes. *)

let is_js cell = String.contains cell ':' && dispatch cell = None
let is_sql cell = class_of cell = "Sql" || is_js cell

let valid_class c =
  let n = String.length c in
  n > 0 && n <= 64 && String.for_all (function 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' -> true | _ -> false) c

let in_savepoint (s : Store.t) f =
  match s.sql with
  | None -> f ()
  | Some db -> (
      ignore (Celld_sqlite.exec db "SAVEPOINT celld_req");
      match f () with
      | Ok _ as r -> ignore (Celld_sqlite.exec db "RELEASE celld_req"); r
      | Error _ as r -> ignore (Celld_sqlite.exec db "ROLLBACK TO celld_req; RELEASE celld_req"); r)

let js (rt : Celld_js.t) cell : handler =
  let on_alarm s =
    match in_savepoint s (fun () -> Celld_js.call rt ~name:"alarm" ~cell ~meth:"" ~path:"" ~query:[] ~body:"") with
    | Ok _ -> ()
    | Error e -> prerr_endline ("alarm handler failed for " ^ cell ^ ": " ^ e)
  in
  with_alarm on_alarm (fun s ~meth ~path ~query ~body ->
      match in_savepoint s (fun () -> Celld_js.call rt ~name:"fetch" ~cell ~meth ~path ~query ~body) with
      | Ok r -> { status = r.status; body = r.body; wrote = Store.changed s; content_type = r.content_type }
      | Error e -> err 500 ("uncaught exception: " ^ e ^ "\n"))
