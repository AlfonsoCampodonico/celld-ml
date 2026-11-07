(* Native cell classes, standing in for Durable Object classes. A handler
   mutates the store's map and says whether it wrote. The runtime commits
   the write, which yields the output gate's position. *)

module M = Store.M

type reply = { status : int; body : string; wrote : bool }

let ok ?(wrote = false) body = { status = 200; body; wrote }
let err status body = { status; body; wrote = false }

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

let alarm_key = "\000alarm"
let alarm_of (s : Store.t) = Option.bind (M.find_opt alarm_key s.kv) int_of_string_opt

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
          s.kv <- M.add alarm_key (string_of_int at) s.kv;
          ok ~wrote:true (string_of_int at ^ "\n")
      | None -> err 400 "PUT /__alarm?in_ms=N or ?at_ms=T\n")
  | "DELETE", "/__alarm" ->
      if alarm_of s = None then ok "none\n"
      else begin
        s.kv <- M.remove alarm_key s.kv;
        ok ~wrote:true "cleared\n"
      end
  | "ALARM", "/__alarm/fire" -> (
      match alarm_of s with
      | Some at when at <= now_ms ->
          s.kv <- M.remove alarm_key s.kv;
          on_alarm s;
          ok ~wrote:true "fired\n"
      | _ -> ok "not due\n")
  | _ -> h s ~meth ~path ~query ~body

let class_of cell = match String.index_opt cell ':' with Some i -> String.sub cell 0 i | None -> "Counter"

let valid_cell id =
  let n = String.length id in
  n > 0 && n <= 255 && id <> "." && id <> ".."
  && String.for_all (function 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '.' | ':' | '$' | '-' -> true | _ -> false) id

let dispatch cell : handler option =
  match class_of cell with
  | "Counter" -> Some (with_alarm counter_alarm counter)
  | "Kv" -> Some (with_alarm kv_alarm kv)
  | _ -> None
