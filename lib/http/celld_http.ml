type request = {
  meth : string;
  target : string;
  headers : (string * string) list;
  body : string;
}

type response = {
  status : int;
  headers : (string * string) list;
  body : string;
}

type 'a parse = Done of 'a * int | Partial | Error of string

let max_head = 64 * 1024

let header name headers =
  let name = String.lowercase_ascii name in
  let rec go = function
    | [] -> None
    | (k, v) :: rest ->
        if String.lowercase_ascii k = name then Some v else go rest
  in
  go headers

let has_header name headers = header name headers <> None

let remove_header name headers =
  let name = String.lowercase_ascii name in
  List.filter (fun (k, _) -> String.lowercase_ascii k <> name) headers

(* Index of the first occurrence of [sub] in [s] at or after [from]. *)
let find_sub s ~from sub =
  let n = String.length s and m = String.length sub in
  let rec matches i j = j >= m || (s.[i + j] = sub.[j] && matches i (j + 1)) in
  let rec go i =
    if i + m > n then None else if matches i 0 then Some i else go (i + 1)
  in
  go from

let is_ws c = c = ' ' || c = '\t'

let trim s =
  let n = String.length s in
  let i = ref 0 and j = ref (n - 1) in
  while !i < n && is_ws s.[!i] do incr i done;
  while !j >= !i && is_ws s.[!j] do decr j done;
  String.sub s !i (!j - !i + 1)

let strip_cr l =
  let n = String.length l in
  if n > 0 && l.[n - 1] = '\r' then String.sub l 0 (n - 1) else l

exception Bad of string

(* Split the head (without the terminating blank line) into the start line
   and parsed headers. *)
let parse_head head =
  match String.split_on_char '\n' head |> List.map strip_cr with
  | [] -> raise (Bad "empty head")
  | start :: lines ->
      let headers =
        List.filter_map
          (fun l ->
            if l = "" then None
            else
              match String.index_opt l ':' with
              | None -> raise (Bad ("malformed header line: " ^ l))
              | Some 0 -> raise (Bad "empty header name")
              | Some i ->
                  let k = String.sub l 0 i in
                  if String.exists is_ws k then raise (Bad "space in header name");
                  Some (k, trim (String.sub l (i + 1) (String.length l - i - 1))))
          lines
      in
      (start, headers)

(* Returns [Some (head, body_start)] once the full head is present. *)
let split_head buf =
  match find_sub buf ~from:0 "\r\n\r\n" with
  | Some i -> `Head (String.sub buf 0 i, i + 4)
  | None ->
      if String.length buf > max_head then `Too_big else `Partial

let parse_hex s =
  let s = trim s in
  if s = "" then raise (Bad "empty chunk size");
  let v = ref 0 in
  String.iter
    (fun c ->
      let d =
        match c with
        | '0' .. '9' -> Char.code c - 48
        | 'a' .. 'f' -> Char.code c - 87
        | 'A' .. 'F' -> Char.code c - 55
        | _ -> raise (Bad "bad chunk size")
      in
      if !v > max_int lsr 5 then raise (Bad "chunk size overflow");
      v := (!v lsl 4) lor d)
    s;
  !v

(* Decode a chunked body starting at [pos]. [`Done (body, end_pos)]. *)
let decode_chunked buf pos =
  let b = Buffer.create 256 in
  let n = String.length buf in
  let rec chunk pos =
    match find_sub buf ~from:pos "\r\n" with
    | None -> if n - pos > 1024 then raise (Bad "chunk line too long") else `Partial
    | Some eol ->
        let line = String.sub buf pos (eol - pos) in
        let line =
          match String.index_opt line ';' with
          | Some i -> String.sub line 0 i
          | None -> line
        in
        let size = parse_hex line in
        let data = eol + 2 in
        if size = 0 then trailers data
        else if data + size + 2 > n then `Partial
        else if String.sub buf (data + size) 2 <> "\r\n" then
          raise (Bad "missing CRLF after chunk")
        else (
          Buffer.add_substring b buf data size;
          chunk (data + size + 2))
  and trailers pos =
    match find_sub buf ~from:pos "\r\n" with
    | None -> `Partial
    | Some eol -> if eol = pos then `Done (Buffer.contents b, eol + 2) else trailers (eol + 2)
  in
  chunk pos

let is_chunked headers =
  match header "transfer-encoding" headers with
  | Some te ->
      List.exists
        (fun t -> String.lowercase_ascii (trim t) = "chunked")
        (String.split_on_char ',' te)
  | None -> false

let content_length headers =
  match header "content-length" headers with
  | None -> None
  | Some v -> (
      match int_of_string_opt (trim v) with
      | Some n when n >= 0 && String.for_all (function '0' .. '9' -> true | _ -> false) (trim v) -> Some n
      | _ -> raise (Bad "bad content-length"))

(* Shared body framing. [`Eof] means "read to end of input". *)
let read_body ~framing buf body_start headers k =
  let n = String.length buf in
  if is_chunked headers then
    match decode_chunked buf body_start with
    | `Partial -> Partial
    | `Done (body, stop) -> k (remove_header "transfer-encoding" headers) body stop
  else
    match content_length headers with
    | Some len ->
        if body_start + len > n then Partial
        else k headers (String.sub buf body_start len) (body_start + len)
    | None -> (
        match framing with
        | `None -> k headers "" body_start
        | `Eof_partial -> Partial
        | `Eof -> k headers (String.sub buf body_start (n - body_start)) n)

let guard f = try f () with Bad msg -> Error msg

let parse_request buf =
  guard @@ fun () ->
  match split_head buf with
  | `Partial -> Partial
  | `Too_big -> Error "request head too large"
  | `Head (head, body_start) -> (
      let start, headers = parse_head head in
      match String.split_on_char ' ' start with
      | [ meth; target; version ]
        when meth <> "" && target <> ""
             && String.length version >= 5
             && String.sub version 0 5 = "HTTP/" ->
          read_body ~framing:`None buf body_start headers (fun headers body stop ->
              Done ({ meth; target; headers; body }, stop))
      | _ -> Error ("bad request line: " ^ start))

let parse_response_gen ~eof ?(head = false) buf =
  guard @@ fun () ->
  match split_head buf with
  | `Partial -> Partial
  | `Too_big -> Error "response head too large"
  | `Head (h, body_start) -> (
      let start, headers = parse_head h in
      let version, rest =
        match String.index_opt start ' ' with
        | Some i -> (String.sub start 0 i, String.sub start (i + 1) (String.length start - i - 1))
        | None -> (start, "")
      in
      if String.length version < 5 || String.sub version 0 5 <> "HTTP/" then
        raise (Bad ("bad status line: " ^ start));
      let code = match String.index_opt rest ' ' with Some i -> String.sub rest 0 i | None -> rest in
      match int_of_string_opt code with
      | Some status when status >= 100 && status <= 999 && String.length code = 3 ->
          let no_body = head || (status >= 100 && status < 200) || status = 204 || status = 304 in
          let mk headers body stop = Done ({ status; headers; body }, stop) in
          if no_body then mk headers "" body_start
          else
            read_body ~framing:(if eof then `Eof else `Eof_partial) buf body_start headers mk
      | _ -> Error ("bad status line: " ^ start))

let parse_response ?head buf = parse_response_gen ~eof:false ?head buf
let parse_response_eof ?head buf = parse_response_gen ~eof:true ?head buf

let reason = function
  | 100 -> "Continue"
  | 200 -> "OK"
  | 201 -> "Created"
  | 202 -> "Accepted"
  | 204 -> "No Content"
  | 301 -> "Moved Permanently"
  | 302 -> "Found"
  | 304 -> "Not Modified"
  | 400 -> "Bad Request"
  | 401 -> "Unauthorized"
  | 403 -> "Forbidden"
  | 404 -> "Not Found"
  | 405 -> "Method Not Allowed"
  | 408 -> "Request Timeout"
  | 409 -> "Conflict"
  | 411 -> "Length Required"
  | 412 -> "Precondition Failed"
  | 413 -> "Payload Too Large"
  | 421 -> "Misdirected Request"
  | 429 -> "Too Many Requests"
  | 500 -> "Internal Server Error"
  | 501 -> "Not Implemented"
  | 502 -> "Bad Gateway"
  | 503 -> "Service Unavailable"
  | 504 -> "Gateway Timeout"
  | 507 -> "Insufficient Storage"
  | _ -> "Unknown"

let add_headers b headers =
  List.iter
    (fun (k, v) ->
      Buffer.add_string b k;
      Buffer.add_string b ": ";
      Buffer.add_string b v;
      Buffer.add_string b "\r\n")
    headers

let framed_headers headers body =
  let headers = remove_header "transfer-encoding" (remove_header "content-length" headers) in
  headers @ [ ("Content-Length", string_of_int (String.length body)) ]

let serialize_request (r : request) =
  let headers = framed_headers r.headers r.body in
  let headers = if has_header "host" headers then headers else ("Host", "localhost") :: headers in
  let b = Buffer.create (128 + String.length r.body) in
  Buffer.add_string b r.meth;
  Buffer.add_char b ' ';
  Buffer.add_string b r.target;
  Buffer.add_string b " HTTP/1.1\r\n";
  add_headers b headers;
  Buffer.add_string b "\r\n";
  Buffer.add_string b r.body;
  Buffer.contents b

let serialize_response (r : response) =
  let no_body = (r.status >= 100 && r.status < 200) || r.status = 204 || r.status = 304 in
  let headers =
    if no_body then remove_header "content-length" (remove_header "transfer-encoding" r.headers)
    else framed_headers r.headers r.body
  in
  let b = Buffer.create (128 + String.length r.body) in
  Buffer.add_string b "HTTP/1.1 ";
  Buffer.add_string b (string_of_int r.status);
  Buffer.add_char b ' ';
  Buffer.add_string b (reason r.status);
  Buffer.add_string b "\r\n";
  add_headers b headers;
  Buffer.add_string b "\r\n";
  if not no_body then Buffer.add_string b r.body;
  Buffer.contents b

let hex_val c =
  match c with
  | '0' .. '9' -> Some (Char.code c - 48)
  | 'a' .. 'f' -> Some (Char.code c - 87)
  | 'A' .. 'F' -> Some (Char.code c - 55)
  | _ -> None

let pct_decode_gen ~plus s =
  let n = String.length s in
  let b = Buffer.create n in
  let rec go i =
    if i >= n then ()
    else
      match s.[i] with
      | '%' when i + 2 < n -> (
          match (hex_val s.[i + 1], hex_val s.[i + 2]) with
          | Some h, Some l ->
              Buffer.add_char b (Char.chr ((h lsl 4) lor l));
              go (i + 3)
          | _ ->
              Buffer.add_char b '%';
              go (i + 1))
      | '+' when plus ->
          Buffer.add_char b ' ';
          go (i + 1)
      | c ->
          Buffer.add_char b c;
          go (i + 1)
  in
  go 0;
  Buffer.contents b

let pct_decode s = pct_decode_gen ~plus:false s

let pct_encode s =
  let b = Buffer.create (String.length s) in
  String.iter
    (fun c ->
      match c with
      | 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '.' | '_' | '~' | '/' | '-' -> Buffer.add_char b c
      | c -> Buffer.add_string b (Printf.sprintf "%%%02X" (Char.code c)))
    s;
  Buffer.contents b

let path_and_query target =
  match String.index_opt target '?' with
  | None -> (target, [])
  | Some i ->
      let path = String.sub target 0 i in
      let q = String.sub target (i + 1) (String.length target - i - 1) in
      let q = match String.index_opt q '#' with Some j -> String.sub q 0 j | None -> q in
      let params =
        String.split_on_char '&' q
        |> List.filter (fun p -> p <> "")
        |> List.map (fun p ->
               match String.index_opt p '=' with
               | None -> (pct_decode_gen ~plus:true p, "")
               | Some j ->
                   ( pct_decode_gen ~plus:true (String.sub p 0 j),
                     pct_decode_gen ~plus:true (String.sub p (j + 1) (String.length p - j - 1)) ))
      in
      (path, params)

let expected_length buf =
  match find_sub buf ~from:0 "\r\n\r\n" with
  | None -> None
  | Some i -> (
      try
        let _, headers = parse_head (String.sub buf 0 i) in
        if is_chunked headers then None
        else
          match content_length headers with
          | Some n -> Some (i + 4 + n)
          | None -> None
      with Bad _ -> None)
