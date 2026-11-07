(* Native cell classes, standing in for Durable Object classes. A handler
   mutates the store's map and says whether it wrote. The runtime commits
   the write, which yields the output gate's position. *)

module M = Store.M

type reply = { status : int; body : string; wrote : bool; content_type : string; effects : string  (** socket operations for the node *) }

let ok ?(wrote = false) body = { status = 200; body; wrote; content_type = "text/plain"; effects = "" }
let err status body = { status; body; wrote = false; content_type = "text/plain"; effects = "" }

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
let counter_alarm s ~now_ms:_ = bump "value" s; bump "alarms" s
let kv_alarm s ~now_ms:_ = bump "alarms_fired" s

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
          on_alarm s ~now_ms;
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
          | Ok rs -> { status = 200; body = json_result rs; wrote = Store.changed s; content_type = "application/json"; effects = "" }
      in
      match meth, path with
      | "POST", "/query" -> run body
      | "GET", ("" | "/") ->
          run (Printf.sprintf "SELECT name FROM sqlite_master WHERE type = 'table' AND name <> '%s' ORDER BY name" Store.meta_table)
      | _ -> err 404 "sql: GET /, POST /query\n")

let sql_alarm (s : Store.t) ~now_ms:_ =
  Option.iter
    (fun db -> ignore (Celld_sqlite.exec db "CREATE TABLE IF NOT EXISTS alarms_fired(at INTEGER); INSERT INTO alarms_fired VALUES (unixepoch('subsec') * 1000)"))
    s.sql

(* ---- SQL helpers for the runtime's own tables ---- *)

let q = Store.sql_quote

let rows (s : Store.t) query =
  match s.sql with
  | None -> []
  | Some db -> ( match Celld_sqlite.exec db query with Ok r -> r.rows | Error _ -> [])

let scalar s query = match rows s query with [ Some v :: _ ] -> Some v | _ -> None
let scalar_int s query = Option.bind (scalar s query) int_of_string_opt
let run s query = match s.Store.sql with Some db -> ignore (Celld_sqlite.exec db query) | None -> ()

(* ---- the transactional outbox ----
   A cell that sends to a queue writes the message to its own outbox table,
   in the same commit as the rest of the request. Once that commit is
   durable the node delivers the outbox to the queue cell, at least once
   (the queue drops duplicate ids), and acknowledges it here. *)

let outbox_table = "__celld_outbox"

let ensure_outbox s =
  run s (Printf.sprintf "CREATE TABLE IF NOT EXISTS %s(id TEXT PRIMARY KEY, queue TEXT NOT NULL, payload TEXT NOT NULL)" outbox_table)

let outbox_add s ~queue ~payload_sql =
  ensure_outbox s;
  run s (Printf.sprintf "INSERT OR IGNORE INTO %s SELECT json_extract(p, '$.id'), %s, p FROM (SELECT %s AS p)" outbox_table (q queue) payload_sql)

(* (queue, JSON array of payloads, JSON array of ids), one per queue. *)
let outbox_pending (s : Store.t) =
  List.filter_map
    (function
      | [ Some queue; Some payloads; Some ids ] -> Some (queue, payloads, ids)
      | _ -> None)
    (rows s
       (Printf.sprintf
          "SELECT queue, '[' || group_concat(payload, ',') || ']', json_group_array(id) FROM (SELECT * FROM %s ORDER BY rowid LIMIT 500) GROUP BY queue"
          outbox_table))

let with_outbox (h : handler) : handler =
 fun s ~now_ms ~meth ~path ~query ~body ->
  match meth, path with
  | "OUTBOX", "/__outbox/ack" ->
      run s (Printf.sprintf "DELETE FROM %s WHERE id IN (SELECT value FROM json_each(%s))" outbox_table (q body));
      { (ok "acked\n") with wrote = Store.changed s }
  | _ -> h s ~now_ms ~meth ~path ~query ~body

(* ---- Queue: a message queue per cell ----
   Messages wait in SQLite. Delivery is driven by the cell's alarm: when a
   batch is due, the alarm fires and the node leases a batch
   (POST /__queue/lease), runs the consumer class's queue() handler on it,
   and settles the result (POST /__queue/settle). A message is delivered at
   least once; its id stays the same across redeliveries. One that fails
   more than max_retries times moves to the dead-letter queue, if any. *)

let retention_ms = 4 * 86_400_000
let lease_ms = 30_000

let queue_schema s =
  run s
    "CREATE TABLE IF NOT EXISTS q_messages(id TEXT PRIMARY KEY, body TEXT NOT NULL, content_type TEXT NOT NULL, \
     created_at INTEGER NOT NULL, visible_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, lease_until INTEGER, batch TEXT); \
     CREATE TABLE IF NOT EXISTS q_config(k TEXT PRIMARY KEY, v TEXT NOT NULL)"

let qconf s k default = Option.value (scalar s (Printf.sprintf "SELECT v FROM q_config WHERE k = %s" (q k))) ~default
let qconf_int s k default = Option.value (int_of_string_opt (qconf s k (string_of_int default))) ~default

let ready_cond now = Printf.sprintf "visible_at <= %d AND (lease_until IS NULL OR lease_until <= %d)" now now

(* The next time the alarm should fire: now-ish when a full batch is ready,
   else when the oldest waiting message reaches the batch timeout, or a
   lease expires. Never earlier than a second from now, so a pending lease
   by the node is not raced by another fire. *)
let queue_reschedule s ~now_ms =
  let consumer = qconf s "consumer" "" in
  let batch = qconf_int s "max_batch_size" 10 and timeout = qconf_int s "max_batch_timeout_ms" 5000 in
  let ready = Option.value (scalar_int s ("SELECT count(*) FROM q_messages WHERE " ^ ready_cond now_ms)) ~default:0 in
  let waiting = scalar_int s (Printf.sprintf "SELECT min(visible_at) + %d FROM q_messages WHERE lease_until IS NULL OR lease_until <= %d" timeout now_ms) in
  let lease = scalar_int s (Printf.sprintf "SELECT min(lease_until) FROM q_messages WHERE lease_until > %d" now_ms) in
  let oldest = scalar_int s "SELECT min(created_at) FROM q_messages" in
  let candidates =
    (if consumer <> "" && ready >= batch then [ now_ms ] else [])
    @ (if consumer <> "" then Option.to_list waiting else [])
    @ Option.to_list lease
    @ Option.to_list (Option.map (fun c -> c + retention_ms) oldest)
  in
  match candidates with
  | [] -> Store.meta_remove s "alarm"
  | c :: rest -> Store.meta_set s "alarm" (string_of_int (max (now_ms + 1000) (List.fold_left min c rest)))

let queue_stats s ~now_ms =
  Option.value ~default:"{}"
    (scalar s
       (Printf.sprintf
          "SELECT json_object('consumer', (SELECT v FROM q_config WHERE k = 'consumer'), 'ready', (SELECT count(*) FROM q_messages WHERE %s), \
           'leased', (SELECT count(*) FROM q_messages WHERE lease_until > %d), 'delayed', (SELECT count(*) FROM q_messages WHERE visible_at > %d), \
           'total', (SELECT count(*) FROM q_messages))"
          (ready_cond now_ms) now_ms now_ms))

let queue_cell cell : handler =
  let name = match String.index_opt cell ':' with Some i -> String.sub cell (i + 1) (String.length cell - i - 1) | None -> cell in
  let json_reply s body = { status = 200; body = body ^ "\n"; wrote = Store.changed s; content_type = "application/json"; effects = "" } in
  let on_alarm s ~now_ms =
    queue_schema s;
    run s (Printf.sprintf "DELETE FROM q_messages WHERE created_at < %d" (now_ms - retention_ms));
    run s (Printf.sprintf "UPDATE q_messages SET lease_until = NULL, batch = NULL WHERE lease_until <= %d" now_ms);
    queue_reschedule s ~now_ms
  in
  with_outbox
    (with_alarm on_alarm (fun s ~meth ~path ~query:_ ~body ->
         (* with_alarm passes no clock to plain requests; the queue reads the
            one SQLite was given (Celld_sqlite.set_now) *)
         let now_ms = Option.value (scalar_int s "SELECT CAST(unixepoch('subsec') * 1000 AS INTEGER)") ~default:0 in
         queue_schema s;
         match meth, path with
         | "GET", ("" | "/") -> json_reply s (queue_stats s ~now_ms)
         | "POST", "/__queue/send" ->
             run s
               (Printf.sprintf
                  "INSERT OR IGNORE INTO q_messages(id, body, content_type, created_at, visible_at) \
                   SELECT json_extract(value, '$.id'), json_extract(value, '$.body'), coalesce(json_extract(value, '$.content_type'), 'json'), \
                   %d, %d + coalesce(json_extract(value, '$.delay_ms'), 0) FROM json_each(%s)"
                  now_ms now_ms (q body));
             queue_reschedule s ~now_ms;
             json_reply s (Printf.sprintf "{\"queue\":%s,\"accepted\":true}" (json_str name))
         | "POST", "/__queue/config" ->
             run s
               (Printf.sprintf "INSERT OR REPLACE INTO q_config SELECT key, CAST(value AS TEXT) FROM json_each(%s) WHERE key IN \
                  ('consumer','max_batch_size','max_batch_timeout_ms','max_retries','retry_delay_ms','dead_letter_queue')" (q body));
             queue_reschedule s ~now_ms;
             json_reply s (queue_stats s ~now_ms)
         | "POST", "/__queue/lease" ->
             let consumer = qconf s "consumer" "" in
             let batch = qconf_int s "max_batch_size" 10 and timeout = qconf_int s "max_batch_timeout_ms" 5000 in
             let ready = Option.value (scalar_int s ("SELECT count(*) FROM q_messages WHERE " ^ ready_cond now_ms)) ~default:0 in
             let oldest = scalar_int s ("SELECT min(visible_at) FROM q_messages WHERE " ^ ready_cond now_ms) in
             let due = ready >= batch || (match oldest with Some o -> o + timeout <= now_ms | None -> false) in
             if consumer = "" || ready = 0 || not due then json_reply s "{\"messages\":[]}"
             else begin
               let id = Option.value (scalar s "SELECT lower(hex(randomblob(8)))") ~default:"batch" in
               run s
                 (Printf.sprintf
                    "UPDATE q_messages SET attempts = attempts + 1, lease_until = %d, batch = %s WHERE id IN \
                     (SELECT id FROM q_messages WHERE %s ORDER BY visible_at, created_at LIMIT %d)"
                    (now_ms + lease_ms) (q id) (ready_cond now_ms) batch);
               queue_reschedule s ~now_ms;
               json_reply s
                 (Option.value ~default:"{\"messages\":[]}"
                    (scalar s
                       (Printf.sprintf
                          "SELECT json_object('queue', %s, 'consumer', %s, 'batch', %s, 'messages', json_group_array(json_object('id', id, 'body', body, \
                           'contentType', content_type, 'timestamp', created_at, 'attempts', attempts))) FROM (SELECT * FROM q_messages WHERE batch = %s ORDER BY created_at)"
                          (q name) (q consumer) (q id) (q id))))
             end
         | "POST", "/__queue/settle" ->
             let b = q body in
             let batch = Option.value (scalar s (Printf.sprintf "SELECT json_extract(%s, '$.batch')" b)) ~default:"" in
             let failed = scalar s (Printf.sprintf "SELECT json_extract(%s, '$.failed')" b) = Some "1" in
             let retry_delay = qconf_int s "retry_delay_ms" 0 and max_retries = qconf_int s "max_retries" 3 in
             let in_batch = Printf.sprintf "batch = %s AND lease_until > %d" (q batch) now_ms in
             (* explicit retries, with their own delays *)
             run s
               (Printf.sprintf
                  "UPDATE q_messages SET lease_until = NULL, batch = NULL, visible_at = %d + coalesce((SELECT json_extract(r.value, '$.delay_ms') \
                   FROM json_each(%s, '$.retry') r WHERE json_extract(r.value, '$.id') = q_messages.id), %d) \
                   WHERE %s AND id IN (SELECT json_extract(value, '$.id') FROM json_each(%s, '$.retry'))"
                  now_ms b retry_delay in_batch b);
             (* acknowledged *)
             run s (Printf.sprintf "DELETE FROM q_messages WHERE %s AND id IN (SELECT value FROM json_each(%s, '$.ack'))" in_batch b);
             (* the rest: retried when the handler failed, otherwise acknowledged *)
             if failed then run s (Printf.sprintf "UPDATE q_messages SET lease_until = NULL, batch = NULL, visible_at = %d WHERE %s" (now_ms + retry_delay) in_batch)
             else run s (Printf.sprintf "DELETE FROM q_messages WHERE %s" in_batch);
             (* past max_retries: to the dead-letter queue, or dropped *)
             let dlq = qconf s "dead_letter_queue" "" in
             let over = Printf.sprintf "lease_until IS NULL AND attempts > %d" max_retries in
             if dlq <> "" then begin
               ensure_outbox s;
               run s
                 (Printf.sprintf
                    "INSERT OR IGNORE INTO %s SELECT id || '-dlq', %s, json_object('id', id || '-dlq', 'body', body, 'content_type', content_type) \
                     FROM q_messages WHERE %s" outbox_table (q dlq) over)
             end;
             run s ("DELETE FROM q_messages WHERE " ^ over);
             queue_reschedule s ~now_ms;
             json_reply s (queue_stats s ~now_ms)
         | _ -> err 404 "queue: GET / for stats; messages arrive from producers' ctx.queue(name).send()\n"))

let class_of cell = match String.index_opt cell ':' with Some i -> String.sub cell 0 i | None -> "Counter"

let valid_cell id =
  let n = String.length id in
  n > 0 && n <= 255 && id <> "." && id <> ".."
  && String.for_all (function 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '.' | ':' | '$' | '-' -> true | _ -> false) id

(* ---- D1: a SQL database cell for JS bindings ----
   ctx.d1(name) reaches D1:<name> through the node. POST /__d1 runs
   {"stmts":[{"sql","params"}], "batch"} with bound parameters; a batch is
   one transaction. The rest is the Sql: class. *)

let d1 (s : Store.t) ~meth ~path ~query ~body =
  match s.sql, meth, path with
  | Some db, "POST", "/__d1" -> (
      let stmts = rows s (Printf.sprintf "SELECT json_extract(value, '$.sql'), value -> '$.params' FROM json_each(%s, '$.stmts')" (q body)) in
      let batch = scalar s (Printf.sprintf "SELECT json_extract(%s, '$.batch')" (q body)) = Some "1" in
      let params p =
        List.map
          (function
            | [ Some "integer"; Some v ] -> Celld_sqlite.Int (Int64.of_string v)
            | [ Some "real"; Some v ] -> Celld_sqlite.Float (float_of_string v)
            | [ Some "true"; _ ] -> Celld_sqlite.Int 1L
            | [ Some "false"; _ ] -> Celld_sqlite.Int 0L
            | [ Some "null"; _ ] | [ _; None ] -> Celld_sqlite.Null
            | [ _; Some v ] -> Celld_sqlite.Text v
            | _ -> Celld_sqlite.Null)
          (rows s (Printf.sprintf "SELECT type, value FROM json_each(%s)" (q p)))
      in
      let jv = function
        | Celld_sqlite.V_null -> "null"
        | V_int v | V_float v -> v
        | V_text v -> json_str v
      in
      if batch then ignore (Celld_sqlite.exec db "SAVEPOINT celld_d1");
      let rec go acc = function
        | [] -> Ok (List.rev acc)
        | [ Some sql; p ] :: rest -> (
            match Celld_sqlite.exec_bound db sql (match p with Some p -> params p | None -> []) with
            | Error e -> Error e
            | Ok (cols, rs, changes, rowid) ->
                let row r = "{" ^ String.concat "," (List.map2 (fun c v -> json_str c ^ ":" ^ jv v) cols r) ^ "}" in
                go (Printf.sprintf "{\"rows\":[%s],\"meta\":{\"changes\":%d,\"last_row_id\":%Ld}}" (String.concat "," (List.map row rs)) changes rowid :: acc) rest)
        | _ :: _ -> Error "bad statement"
      in
      match go [] stmts with
      | Ok results ->
          if batch then ignore (Celld_sqlite.exec db "RELEASE celld_d1");
          { status = 200; body = "[" ^ String.concat "," results ^ "]"; wrote = Store.changed s; content_type = "application/json"; effects = "" }
      | Error e ->
          if batch then ignore (Celld_sqlite.exec db "ROLLBACK TO celld_d1; RELEASE celld_d1");
          err 400 (e ^ "\n"))
  | _ -> sql s ~meth ~path ~query ~body

let dispatch cell : handler option =
  match class_of cell with
  | "Counter" -> Some (with_alarm counter_alarm counter)
  | "Kv" -> Some (with_alarm kv_alarm kv)
  | "Sql" -> Some (with_alarm sql_alarm sql)
  | "Queue" -> Some (queue_cell cell)
  | "D1" -> Some (with_alarm sql_alarm d1)
  | _ -> None

(* ---- JS classes ----
   Any other class name is a JS class whose script lives in the bucket at
   classes/<Class>.js. Its storage is the cell's SQLite database, and each
   call runs inside a savepoint: a handler that throws leaves no writes. *)

let is_js cell = String.contains cell ':' && dispatch cell = None
let is_sql cell = List.mem (class_of cell) [ "Sql"; "Queue"; "D1" ] || is_js cell

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

(* ---- cron ----
   <Class>:__cron is the class's schedule cell: its alarm is the earliest
   pending occurrence or retry, and it runs scheduled(controller, ctx) once
   per occurrence across the fleet. A failure retries after 4 s, doubling,
   at most six times and never past the next occurrence, unless the handler
   calls controller.noRetry(). After downtime one missed occurrence runs and
   the rest are skipped. *)

let is_cron_cell cell = String.ends_with ~suffix:":__cron" cell

let cron_exprs s =
  match Store.meta_get s "crons" with
  | None -> []
  | Some l -> List.filter_map (fun e -> match Celld_cron.parse e with Ok t -> Some (e, t) | Error _ -> None) (String.split_on_char '\n' l)

let meta_int s k = Option.bind (Store.meta_get s k) int_of_string_opt

let cron_retry s e =
  match Option.map (String.split_on_char ',') (Store.meta_get s ("cron_retry:" ^ e)) with
  | Some [ occ; fails; at ] -> (
      match int_of_string_opt occ, int_of_string_opt fails, int_of_string_opt at with Some o, Some f, Some a -> Some (o, f, a) | _ -> None)
  | _ -> None

let cron_reschedule s ~now_ms =
  let times =
    List.filter_map
      (fun (e, t) ->
        match cron_retry s e with
        | Some (_, _, at) -> Some at
        | None -> Celld_cron.next t ~after_ms:(max now_ms (Option.value (meta_int s ("cron_last:" ^ e)) ~default:now_ms)))
      (cron_exprs s)
  in
  match times with [] -> Store.meta_remove s "alarm" | t :: r -> Store.meta_set s "alarm" (string_of_int (List.fold_left min t r))

(* ---- JS cells: calls that may wait for I/O ----
   A JS call either replies or awaits operations (fetch, cell calls, D1, R2,
   timers, workflows) that the node performs; [Await (ops, k)] resumes it
   with their results. One savepoint covers the whole call, across every
   wait, so a handler that throws leaves no writes. *)

type step = Done of reply | Await of string * (string -> step)

type async_handler =
  Store.t -> now_ms:int -> meth:string -> path:string -> query:(string * string) list -> body:string -> step

let reply_of_js ?(content_type = "") s (r : Celld_js.response) =
  { status = r.status; body = r.body; wrote = Store.changed s; content_type = (if content_type = "" then Celld_js.content_type r else content_type); effects = r.effects }

(* Run one JS call to completion. [strict]: a 5xx from a runtime-dispatched
   handler (scheduled, queue) means it threw. *)
let js_call s rt ~strict ~name ~cell ~meth ~path ~query ~body (k : (Celld_js.response, Celld_js.response) result -> step) : step =
  let sp q = match s.Store.sql with Some db -> ignore (Celld_sqlite.exec db q) | None -> () in
  sp "SAVEPOINT celld_req";
  let finish res =
    (match res with Ok _ -> sp "RELEASE celld_req" | Error _ -> sp "ROLLBACK TO celld_req; RELEASE celld_req");
    k res
  in
  let rec go = function
    | Ok (Celld_js.Reply r) -> if strict && r.status >= 500 then finish (Error r) else finish (Ok r)
    | Ok (Celld_js.Await ops) -> Await (ops, fun results -> go (Celld_js.resume rt results))
    | Error e -> finish (Error { Celld_js.status = 500; headers = [ ("content-type", "text/plain") ]; body = e; effects = "" })
  in
  go (Celld_js.call rt ~name ~cell ~meth ~path ~query ~body)

let is_wf_cell cell = match String.index_opt cell ':' with Some i -> String.length cell > i + 4 && String.sub cell (i + 1) 3 = "wf-" | None -> false

let jx s json path = scalar s (Printf.sprintf "SELECT json_extract(%s, %s)" (q json) (q path))
let jraw s json path = scalar s (Printf.sprintf "SELECT %s -> %s" (q json) (q path))

let js (rt : Celld_js.t) cell : async_handler =
  let fired s = Done { (ok "fired\n") with wrote = Store.changed s } in
  (* cron: run each due expression in turn, then re-arm *)
  let cron_tick s ~now_ms =
    let rec each = function
      | [] -> cron_reschedule s ~now_ms; fired s
      | (e, t) :: rest -> (
          let run occ fails =
            js_call s rt ~strict:true ~name:"scheduled" ~cell ~meth:"" ~path:"" ~query:[]
              ~body:(Printf.sprintf "{\"cron\":%s,\"scheduledTime\":%d}" (json_str e) occ)
              (fun res ->
                (match res with
                | Ok _ ->
                    Store.meta_set s ("cron_last:" ^ e) (string_of_int occ);
                    Store.meta_remove s ("cron_retry:" ^ e)
                | Error r ->
                    let no_retry = jx s r.body "$.noRetry" = Some "1" in
                    let fails = fails + 1 in
                    let retry_at = now_ms + (4000 * (1 lsl (fails - 1))) in
                    let past_next = match Celld_cron.next t ~after_ms:occ with Some n -> retry_at >= n | None -> false in
                    prerr_endline (Printf.sprintf "cron %s of %s failed (attempt %d)" e cell fails);
                    if no_retry || fails >= 6 || past_next then begin
                      Store.meta_set s ("cron_last:" ^ e) (string_of_int occ);
                      Store.meta_remove s ("cron_retry:" ^ e)
                    end
                    else Store.meta_set s ("cron_retry:" ^ e) (Printf.sprintf "%d,%d,%d" occ fails retry_at));
                each rest)
          in
          match cron_retry s e with
          | Some (occ, fails, at) when at <= now_ms -> (
              match Celld_cron.latest_due t ~after_ms:occ ~at_ms:now_ms with Some newer -> run newer 0 | None -> run occ fails)
          | Some _ -> each rest
          | None -> (
              let last = Option.value (meta_int s ("cron_last:" ^ e)) ~default:now_ms in
              match Celld_cron.latest_due t ~after_ms:last ~at_ms:now_ms with Some occ -> run occ 0 | None -> each rest))
    in
    each (cron_exprs s)
  in
  (* workflows: each alarm runs the instance from the top; finished steps
     come back from storage, and a sleep or a retry suspends it again *)
  let wf_status s =
    Printf.sprintf "{\"id\":%s,\"status\":%s,\"output\":%s,\"error\":%s}"
      (json_str cell)
      (json_str (Option.value (Store.meta_get s "wf_status") ~default:"unknown"))
      (Option.value (Store.meta_get s "wf_output") ~default:"null")
      (match Store.meta_get s "wf_error" with Some e -> json_str e | None -> "null")
  in
  let wf_run s ~now_ms =
    match Store.meta_get s "wf_status" with
    | Some ("queued" | "waiting" | "running") ->
        Store.meta_set s "wf_status" "running";
        let input =
          Printf.sprintf "{\"id\":%s,\"params\":%s,\"created\":%s}" (json_str cell)
            (Option.value (Store.meta_get s "wf_params") ~default:"null")
            (Option.value (Store.meta_get s "wf_created") ~default:"0")
        in
        js_call s rt ~strict:false ~name:"workflow" ~cell ~meth:"" ~path:"" ~query:[] ~body:input (fun res ->
            (match res with
            | Ok r -> (
                match jx s r.body "$.status" with
                | Some "complete" ->
                    Store.meta_set s "wf_status" "complete";
                    Store.meta_set s "wf_output" (Option.value (jraw s r.body "$.output") ~default:"null")
                | Some "waiting" ->
                    Store.meta_set s "wf_status" "waiting";
                    Store.meta_set s "alarm" (Option.value (jx s r.body "$.wake") ~default:(string_of_int (now_ms + 1000)))
                | _ ->
                    Store.meta_set s "wf_status" "errored";
                    Store.meta_set s "wf_error" (Option.value (jx s r.body "$.error") ~default:"unknown error"))
            | Error r ->
                Store.meta_set s "wf_status" "errored";
                Store.meta_set s "wf_error" r.body);
            fired s)
    | _ -> fired s
  in
  fun s ~now_ms ~meth ~path ~query ~body ->
    match meth, path with
    | ("GET" | "PUT" | "DELETE"), "/__alarm" -> Done (with_alarm (fun _ ~now_ms:_ -> ()) (fun _ ~meth:_ ~path:_ ~query:_ ~body:_ -> err 404 "") s ~now_ms ~meth ~path ~query ~body)
    | "ALARM", "/__alarm/fire" -> (
        match alarm_of s with
        | Some at when at <= now_ms ->
            Store.meta_remove s "alarm";
            if is_cron_cell cell then cron_tick s ~now_ms
            else if is_wf_cell cell then wf_run s ~now_ms
            else js_call s rt ~strict:true ~name:"alarm" ~cell ~meth:"" ~path:"" ~query:[] ~body:"" (fun res ->
                (match res with Error r -> prerr_endline ("alarm handler failed for " ^ cell ^ ": " ^ r.body) | Ok _ -> ());
                match res with Ok r | Error r -> Done { (ok "fired\n") with wrote = Store.changed s; effects = r.effects })
        | _ -> Done (ok "not due\n"))
    | "OUTBOX", "/__outbox/ack" -> Done (with_outbox (fun _ ~now_ms:_ ~meth:_ ~path:_ ~query:_ ~body:_ -> err 404 "") s ~now_ms ~meth ~path ~query ~body)
    | "CRON", "/__cron/arm" when is_cron_cell cell ->
        let exprs = List.filter (fun e -> String.trim e <> "") (List.map String.trim (String.split_on_char '\n' body)) in
        Store.meta_set s "crons" (String.concat "\n" exprs);
        (* a new schedule starts now: no backfill *)
        List.iter (fun e -> if meta_int s ("cron_last:" ^ e) = None then Store.meta_set s ("cron_last:" ^ e) (string_of_int now_ms)) exprs;
        cron_reschedule s ~now_ms;
        Done { (ok (Printf.sprintf "armed %d schedules\n" (List.length exprs))) with wrote = Store.changed s }
    | "WORKFLOW", "/__wf/create" when is_wf_cell cell ->
        if Store.meta_get s "wf_status" = None then begin
          Store.meta_set s "wf_params" (Option.value (jraw s body "$.params") ~default:"null");
          Store.meta_set s "wf_created" (string_of_int now_ms);
          Store.meta_set s "wf_status" "queued";
          Store.meta_set s "alarm" (string_of_int now_ms)
        end;
        Done { status = 200; body = wf_status s; wrote = Store.changed s; content_type = "application/json"; effects = "" }
    | "WORKFLOW", "/__wf/status" when is_wf_cell cell ->
        Done { status = 200; body = wf_status s; wrote = false; content_type = "application/json"; effects = "" }
    | "WS", "/__ws/touch" -> Done (ok "ok\n")
    | "WS", (("/__ws/message" | "/__ws/close") as p) ->
        let name = if p = "/__ws/message" then "ws_message" else "ws_close" in
        js_call s rt ~strict:false ~name ~cell ~meth:"" ~path:"" ~query:[] ~body (function
          | Ok r -> Done { (ok "") with wrote = Store.changed s; effects = r.effects }
          | Error r -> Done (err 500 ("uncaught exception: " ^ r.body ^ "\n")))
    | "QUEUE", "/__queue/deliver" ->
        js_call s rt ~strict:true ~name:"queue" ~cell ~meth:"" ~path:"" ~query:[] ~body (function
          | Ok r -> Done (reply_of_js ~content_type:"application/json" s r)
          | Error r -> Done { (reply_of_js ~content_type:"application/json" s r) with status = 500 })
    | _ ->
        js_call s rt ~strict:false ~name:"fetch" ~cell ~meth ~path ~query ~body (function
          | Ok r -> Done (reply_of_js s r)
          | Error r -> Done (err 500 ("uncaught exception: " ^ r.body ^ "\n")))


(* ---- JS cells on the event loop ----
   Regular JS cells run on the node's event loop: calls interleave whenever
   one awaits, and writes apply as they happen (ctx.storage.transactionSync
   and transaction group them). This maps a request to either an immediate
   reply or a JS call and how to turn its output into a reply. The
   runtime-driven cells (cron, queue consumers, workflows) keep [js]. *)

type js_route =
  | Reply_now of reply
  | Call of { name : string; body : string; finish : (Celld_js.response, string) result -> reply }

let is_strict_js_cell cell = is_cron_cell cell || is_wf_cell cell || String.ends_with ~suffix:":__consumer" cell

let js_loop_route s ~now_ms ~meth ~path ~query ~body =
  let plain _ ~meth:_ ~path:_ ~query:_ ~body:_ = err 404 "" in
  match meth, path with
  | ("GET" | "PUT" | "DELETE"), "/__alarm" -> Reply_now (with_alarm (fun _ ~now_ms:_ -> ()) plain s ~now_ms ~meth ~path ~query ~body)
  | "ALARM", "/__alarm/fire" -> (
      match alarm_of s with
      | Some at when at <= now_ms ->
          Store.meta_remove s "alarm";
          Call { name = "alarm"; body = ""; finish = (fun _ -> ok "fired\n") }
      | _ -> Reply_now (ok "not due\n"))
  | "OUTBOX", "/__outbox/ack" -> Reply_now (with_outbox (fun _ ~now_ms:_ ~meth:_ ~path:_ ~query:_ ~body:_ -> err 404 "") s ~now_ms ~meth ~path ~query ~body)
  | "WS", "/__ws/touch" -> Reply_now (ok "ok\n")
  | "WS", (("/__ws/message" | "/__ws/close") as p) ->
      Call
        {
          name = (if p = "/__ws/message" then "ws_message" else "ws_close");
          body;
          finish = (function Ok _ -> ok "" | Error e -> err 500 ("uncaught exception: " ^ e ^ "\n"));
        }
  | m, _ when String.length m > 0 && not (List.mem m [ "GET"; "HEAD"; "POST"; "PUT"; "PATCH"; "DELETE"; "OPTIONS" ]) -> Reply_now (err 405 "not a JS cell event\n")
  | _ ->
      Call
        {
          name = "fetch";
          body;
          finish =
            (function
            | Ok r -> { status = r.status; body = r.body; wrote = false; content_type = Celld_js.content_type r; effects = "" }
            | Error e -> err 500 ("uncaught exception: " ^ e ^ "\n"));
        }
