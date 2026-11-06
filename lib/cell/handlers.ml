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

(* Kv: GET / lists, GET|PUT|DELETE /<key>. *)
let kv (s : Store.t) ~meth ~path ~query:_ ~body =
  let key = if String.length path > 1 then String.sub path 1 (String.length path - 1) else "" in
  match meth, key with
  | "GET", "" -> ok (String.concat "" (List.map (fun (k, _) -> k ^ "\n") (M.bindings s.kv)))
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

let class_of cell = match String.index_opt cell ':' with Some i -> String.sub cell 0 i | None -> "Counter"

let valid_cell id =
  let n = String.length id in
  n > 0 && n <= 255 && id <> "." && id <> ".."
  && String.for_all (function 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '.' | ':' | '$' | '-' -> true | _ -> false) id

let dispatch cell =
  match class_of cell with "Counter" -> Some counter | "Kv" -> Some kv | _ -> None
