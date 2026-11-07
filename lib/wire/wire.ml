(* Bucket record codecs and the node-log batch format. Pure, so the native
   tests cover them. *)

open Celld_logic

(* ---- a JSON just big enough for our records ---- *)

type json = Null | Bool of bool | Int of int | Str of string | Arr of json list | Obj of (string * json) list

let rec to_string = function
  | Null -> "null"
  | Bool b -> string_of_bool b
  | Int n -> string_of_int n
  | Str s ->
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
  | Arr l -> "[" ^ String.concat "," (List.map to_string l) ^ "]"
  | Obj l -> "{" ^ String.concat "," (List.map (fun (k, v) -> to_string (Str k) ^ ":" ^ to_string v) l) ^ "}"

exception Bad of string

let of_string s =
  let n = String.length s in
  let pos = ref 0 in
  let peek () = if !pos < n then s.[!pos] else '\000' in
  let rec ws () = if !pos < n && (s.[!pos] = ' ' || s.[!pos] = '\n' || s.[!pos] = '\t' || s.[!pos] = '\r') then (incr pos; ws ()) in
  let expect c = ws (); if peek () <> c then raise (Bad (Printf.sprintf "expected %c at %d" c !pos)); incr pos in
  let lit w v = if !pos + String.length w <= n && String.sub s !pos (String.length w) = w then (pos := !pos + String.length w; v) else raise (Bad "literal") in
  let str () =
    expect '"';
    let b = Buffer.create 16 in
    let rec go () =
      if !pos >= n then raise (Bad "unterminated string");
      let c = s.[!pos] in
      incr pos;
      match c with
      | '"' -> ()
      | '\\' ->
          let e = peek () in
          incr pos;
          (match e with
          | 'n' -> Buffer.add_char b '\n'
          | 't' -> Buffer.add_char b '\t'
          | 'u' ->
              let code = int_of_string ("0x" ^ String.sub s !pos 4) in
              pos := !pos + 4;
              Buffer.add_char b (Char.chr (code land 0xff))
          | c -> Buffer.add_char b c);
          go ()
      | c -> Buffer.add_char b c; go ()
    in
    go ();
    Buffer.contents b
  in
  let rec value () =
    ws ();
    match peek () with
    | '{' ->
        incr pos;
        ws ();
        if peek () = '}' then (incr pos; Obj [])
        else
          let rec fields acc =
            let k = str () in
            expect ':';
            let v = value () in
            ws ();
            match peek () with
            | ',' -> incr pos; fields ((k, v) :: acc)
            | '}' -> incr pos; Obj (List.rev ((k, v) :: acc))
            | _ -> raise (Bad "object")
          in
          fields []
    | '[' ->
        incr pos;
        ws ();
        if peek () = ']' then (incr pos; Arr [])
        else
          let rec items acc =
            let v = value () in
            ws ();
            match peek () with
            | ',' -> incr pos; items (v :: acc)
            | ']' -> incr pos; Arr (List.rev (v :: acc))
            | _ -> raise (Bad "array")
          in
          items []
    | '"' -> Str (str ())
    | 't' -> lit "true" (Bool true)
    | 'f' -> lit "false" (Bool false)
    | 'n' -> lit "null" Null
    | _ ->
        let start = !pos in
        if peek () = '-' then incr pos;
        while !pos < n && s.[!pos] >= '0' && s.[!pos] <= '9' do incr pos done;
        (match int_of_string_opt (String.sub s start (!pos - start)) with Some i -> Int i | None -> raise (Bad "number"))
  in
  match value () with v -> Ok v | exception Bad e -> Error e | exception _ -> Error "malformed json"

let field k = function Obj l -> List.assoc_opt k l | _ -> None
let str_field k j = match field k j with Some (Str s) -> s | _ -> ""
let int_field k j = match field k j with Some (Int i) -> i | _ -> 0

(* ---- cells/<cell>/own.json ---- *)

let owner_to_json ~node ~epoch = to_string (Obj [ ("node", Str (Option.value node ~default:"")); ("epoch", Int epoch) ])

let owner_of_json body ~etag =
  match of_string body with
  | Ok j ->
      let n = str_field "node" j in
      Ok { o_node = (if n = "" then None else Some n); o_epoch = int_field "epoch" j; o_etag = etag }
  | Error e -> Error e

(* ---- nodes/<node>.json ---- *)

let lease_to_json (l : lease) =
  to_string
    (Obj
       [
         ("node", Str l.l_node);
         ("session", Str l.l_session);
         ("addr", Str l.l_addr);
         ("expires_ms", Int l.l_expires_ms);
         ("protocol", Int l.l_protocol);
         ("log", Str (string_of_log_state l.l_log));
         ("followers", Arr (List.map (fun p -> Arr [ Str p.p_node; Str p.p_addr ]) l.l_followers));
         ("cells", Int l.l_cells);
         ("draining", Bool l.l_draining);
       ])

let lease_of_json body ~etag =
  match of_string body with
  | Error e -> Error e
  | Ok j ->
      let followers =
        match field "followers" j with
        | Some (Arr l) -> List.filter_map (function Arr [ Str n; Str a ] -> Some { p_node = n; p_addr = a } | _ -> None) l
        | _ -> []
      in
      Ok
        {
          l_node = str_field "node" j;
          l_session = str_field "session" j;
          l_addr = str_field "addr" j;
          l_expires_ms = int_field "expires_ms" j;
          l_protocol = int_field "protocol" j;
          l_log = log_state_of_string (str_field "log" j);
          l_followers = followers;
          l_cells = int_field "cells" j;
          l_draining = (match field "draining" j with Some (Bool b) -> b | _ -> false);
          l_etag = etag;
        }

(* ---- node-log batches: celld's CLA1 append body (little-endian) ----
   "CLA1" u16 leader_len leader u64 epoch u64 truncate_to u32 count entry*
   entry = "CLE1" u64 seq u16 cell_len cell u64 cell_epoch u64 txid u32 len bytes
   [leader] is the leader's session; [epoch] is unused here (0). *)

type entry = { seq : int; cell : cell_id; cell_epoch : int; txid : int64; ltx : string }
type batch = { leader : string; truncate_to : int; entries : entry list }

let encode_batch b =
  let buf = Buffer.create 1024 in
  let u16 n = Buffer.add_uint16_le buf n and u32 n = Buffer.add_int32_le buf (Int32.of_int n) and u64 n = Buffer.add_int64_le buf n in
  Buffer.add_string buf "CLA1";
  u16 (String.length b.leader);
  Buffer.add_string buf b.leader;
  u64 0L;
  u64 (Int64.of_int b.truncate_to);
  u32 (List.length b.entries);
  List.iter
    (fun e ->
      Buffer.add_string buf "CLE1";
      u64 (Int64.of_int e.seq);
      u16 (String.length e.cell);
      Buffer.add_string buf e.cell;
      u64 (Int64.of_int e.cell_epoch);
      u64 e.txid;
      u32 (String.length e.ltx);
      Buffer.add_string buf e.ltx)
    b.entries;
  Buffer.contents buf

let decode_batch s =
  let pos = ref 0 in
  let take n = let v = String.sub s !pos n in pos := !pos + n; v in
  let u16 () = let v = String.get_uint16_le s !pos in pos := !pos + 2; v in
  let u32 () = let v = Int32.to_int (String.get_int32_le s !pos) in pos := !pos + 4; v in
  let u64 () = let v = String.get_int64_le s !pos in pos := !pos + 8; v in
  try
    if take 4 <> "CLA1" then Error "bad batch magic"
    else
      let leader = take (u16 ()) in
      let _epoch = u64 () in
      let truncate_to = Int64.to_int (u64 ()) in
      let count = u32 () in
      let entries =
        List.init count (fun _ ->
            if take 4 <> "CLE1" then raise Exit;
            let seq = Int64.to_int (u64 ()) in
            let cell = take (u16 ()) in
            let cell_epoch = Int64.to_int (u64 ()) in
            let txid = u64 () in
            let ltx = take (u32 ()) in
            { seq; cell; cell_epoch; txid; ltx })
      in
      if !pos <> String.length s then Error "trailing bytes" else Ok { leader; truncate_to; entries }
  with _ -> Error "malformed batch"

(* ---- bucket keys ---- *)

let own_key cell = "cells/" ^ cell ^ "/own.json"
let lease_key node = "nodes/" ^ node ^ ".json"
let ltx_prefix cell = "cells/" ^ cell ^ "/ltx/"
let peer_auth_key = "fleet/peer-auth.json"

(* ---- peer request signing (HMAC is supplied by the caller) ---- *)

let canonical ~meth ~path ~body_sha256 ~source ~timestamp ~nonce =
  String.concat "\n" [ "cells-peer-request-v1"; "5"; meth; path; body_sha256; source; timestamp; nonce ]

let hex s = String.concat "" (List.init (String.length s) (fun i -> Printf.sprintf "%02x" (Char.code s.[i])))

(* Constant-time comparison for signatures. *)
let equal_ct a b =
  String.length a = String.length b
  && (let acc = ref 0 in
      String.iteri (fun i c -> acc := !acc lor (Char.code c lxor Char.code b.[i])) a;
      !acc = 0)
