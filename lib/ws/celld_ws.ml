(* WebSocket (RFC 6455), server side: the opening handshake's accept key,
   and frames. Pure, so the native tests check them against the RFC. *)

let base64 s =
  let tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/" in
  let b = Buffer.create ((String.length s + 2) / 3 * 4) in
  let n = String.length s in
  let rec go i =
    if i < n then begin
      let c k = if i + k < n then Char.code s.[i + k] else 0 in
      let v = (c 0 lsl 16) lor (c 1 lsl 8) lor c 2 in
      Buffer.add_char b tbl.[(v lsr 18) land 63];
      Buffer.add_char b tbl.[(v lsr 12) land 63];
      Buffer.add_char b (if i + 1 < n then tbl.[(v lsr 6) land 63] else '=');
      Buffer.add_char b (if i + 2 < n then tbl.[v land 63] else '=');
      go (i + 3)
    end
  in
  go 0;
  Buffer.contents b

let accept_key key = base64 Digestif.SHA1.(to_raw_string (digest_string (String.trim key ^ "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")))

type opcode = Continuation | Text | Binary | Close | Ping | Pong | Other of int

let opcode_of = function 0 -> Continuation | 1 -> Text | 2 -> Binary | 8 -> Close | 9 -> Ping | 10 -> Pong | n -> Other n
let int_of_opcode = function Continuation -> 0 | Text -> 1 | Binary -> 2 | Close -> 8 | Ping -> 9 | Pong -> 10 | Other n -> n

type frame = { fin : bool; opcode : opcode; payload : string }

(* A server frame: never masked. *)
let encode ?(fin = true) opcode payload =
  let n = String.length payload in
  let b = Buffer.create (n + 10) in
  Buffer.add_char b (Char.chr ((if fin then 0x80 else 0) lor int_of_opcode opcode));
  if n < 126 then Buffer.add_char b (Char.chr n)
  else if n < 65536 then (Buffer.add_char b (Char.chr 126); Buffer.add_uint16_be b n)
  else (Buffer.add_char b (Char.chr 127); Buffer.add_int64_be b (Int64.of_int n));
  Buffer.add_string b payload;
  Buffer.contents b

let close_frame code reason =
  let b = Bytes.create 2 in
  Bytes.set_uint16_be b 0 code;
  encode Close (Bytes.to_string b ^ reason)

(* Parse one frame from the front of [s]. Clients must mask their frames.
   [`Partial]: need more bytes. *)
let decode ?(max_payload = 1 lsl 20) s =
  let n = String.length s in
  if n < 2 then `Partial
  else
    let b0 = Char.code s.[0] and b1 = Char.code s.[1] in
    let masked = b1 land 0x80 <> 0 and len7 = b1 land 0x7f in
    if b0 land 0x70 <> 0 then `Error "reserved bits set"
    else if not masked then `Error "client frames must be masked"
    else
      let hdr, len =
        if len7 < 126 then (2, Some len7)
        else if len7 = 126 then (4, if n >= 4 then Some (String.get_uint16_be s 2) else None)
        else (10, if n >= 10 then Some (Int64.to_int (String.get_int64_be s 2)) else None)
      in
      match len with
      | None -> `Partial
      | Some len when len < 0 || len > max_payload -> `Error "frame too large"
      | Some len ->
          let total = hdr + 4 + len in
          if n < total then `Partial
          else
            let mask = String.sub s hdr 4 in
            let payload = String.init len (fun i -> Char.chr (Char.code s.[hdr + 4 + i] lxor Char.code mask.[i land 3])) in
            let opcode = opcode_of (b0 land 0x0f) in
            let control = match opcode with Close | Ping | Pong -> true | _ -> false in
            if control && (len > 125 || b0 land 0x80 = 0) then `Error "bad control frame"
            else `Frame ({ fin = b0 land 0x80 <> 0; opcode; payload }, total)

(* What a client close frame carries. *)
let close_info payload =
  if String.length payload >= 2 then (String.get_uint16_be payload 0, String.sub payload 2 (String.length payload - 2)) else (1005, "")

let base64_decode s =
  let v c = match c with 'A' .. 'Z' -> Char.code c - 65 | 'a' .. 'z' -> Char.code c - 71 | '0' .. '9' -> Char.code c + 4 | '+' -> 62 | '/' -> 63 | _ -> -1 in
  let b = Buffer.create (String.length s * 3 / 4) in
  let acc = ref 0 and bits = ref 0 in
  String.iter
    (fun c ->
      let x = v c in
      if x >= 0 then begin
        acc := (!acc lsl 6) lor x;
        bits := !bits + 6;
        if !bits >= 8 then begin
          bits := !bits - 8;
          Buffer.add_char b (Char.chr ((!acc lsr !bits) land 0xff))
        end
      end)
    s;
  Buffer.contents b
