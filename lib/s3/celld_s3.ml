(* S3 wire details for the bucket client: AWS Signature Version 4, object
   key encoding, and ListObjectsV2 parsing. Pure, so the native tests check
   them against AWS's published examples. *)

let sha256_hex s = Digestif.SHA256.(to_hex (digest_string s))
let hmac key s = Digestif.SHA256.(to_raw_string (hmac_string ~key s))
let empty_sha256 = sha256_hex ""

(* RFC 3986 unreserved characters pass; everything else is %XX. [slash]
   keeps '/' for object paths. *)
let uri_encode ?(slash = false) s =
  let b = Buffer.create (String.length s * 3) in
  String.iter
    (fun c ->
      match c with
      | 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '-' | '_' | '.' | '~' -> Buffer.add_char b c
      | '/' when slash -> Buffer.add_char b c
      | c -> Buffer.add_string b (Printf.sprintf "%%%02X" (Char.code c)))
    s;
  Buffer.contents b

type credentials = { access_key : string; secret_key : string; region : string }

(* [amz_date] is "YYYYMMDDTHHMMSSZ". [path] is the encoded URI path, [query]
   unencoded pairs, [headers] everything to sign besides host, date and
   payload hash. Returns the headers to send (including those three and
   Authorization). *)
let sign creds ~amz_date ~meth ~host ~path ~query ~headers ~payload_sha256 =
  let date = String.sub amz_date 0 8 in
  let scope = String.concat "/" [ date; creds.region; "s3"; "aws4_request" ] in
  let all =
    ("host", host) :: ("x-amz-content-sha256", payload_sha256) :: ("x-amz-date", amz_date)
    :: List.map (fun (k, v) -> (String.lowercase_ascii k, String.trim v)) headers
    |> List.sort (fun (a, _) (b, _) -> compare a b)
  in
  let signed_headers = String.concat ";" (List.map fst all) in
  let canonical_query =
    List.map (fun (k, v) -> (uri_encode k, uri_encode v)) query
    |> List.sort compare
    |> List.map (fun (k, v) -> k ^ "=" ^ v)
    |> String.concat "&"
  in
  let canonical =
    String.concat "\n"
      [ meth; path; canonical_query; String.concat "" (List.map (fun (k, v) -> k ^ ":" ^ v ^ "\n") all); signed_headers; payload_sha256 ]
  in
  let to_sign = String.concat "\n" [ "AWS4-HMAC-SHA256"; amz_date; scope; sha256_hex canonical ] in
  let key = List.fold_left hmac ("AWS4" ^ creds.secret_key) [ date; creds.region; "s3"; "aws4_request" ] in
  let signature = Digestif.SHA256.(to_hex (hmac_string ~key to_sign)) in
  let auth =
    Printf.sprintf "AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s" creds.access_key scope signed_headers signature
  in
  [ ("Host", host); ("x-amz-date", amz_date); ("x-amz-content-sha256", payload_sha256); ("Authorization", auth) ] @ headers

(* Seconds since the epoch -> "YYYYMMDDTHHMMSSZ" (UTC). *)
let amz_date_of_unix t =
  let days = t / 86400 and secs = t mod 86400 in
  (* civil_from_days (Howard Hinnant's algorithm). *)
  let z = days + 719468 in
  let era = (if z >= 0 then z else z - 146096) / 146097 in
  let doe = z - (era * 146097) in
  let yoe = (doe - (doe / 1460) + (doe / 36524) - (doe / 146096)) / 365 in
  let y = yoe + (era * 400) in
  let doy = doe - ((365 * yoe) + (yoe / 4) - (yoe / 100)) in
  let mp = ((5 * doy) + 2) / 153 in
  let d = doy - (((153 * mp) + 2) / 5) + 1 in
  let m = if mp < 10 then mp + 3 else mp - 9 in
  let y = if m <= 2 then y + 1 else y in
  Printf.sprintf "%04d%02d%02dT%02d%02d%02dZ" y m d (secs / 3600) (secs / 60 mod 60) (secs mod 60)

(* ---- ListObjectsV2 ---- *)

let xml_unescape s =
  let b = Buffer.create (String.length s) in
  let n = String.length s in
  let rec go i =
    if i >= n then ()
    else if s.[i] = '&' then (
      match String.index_from_opt s i ';' with
      | Some j ->
          (match String.sub s (i + 1) (j - i - 1) with
          | "amp" -> Buffer.add_char b '&'
          | "lt" -> Buffer.add_char b '<'
          | "gt" -> Buffer.add_char b '>'
          | "quot" -> Buffer.add_char b '"'
          | "apos" -> Buffer.add_char b '\''
          | e when String.length e > 1 && e.[0] = '#' -> (
              let code = if e.[1] = 'x' then int_of_string_opt ("0" ^ String.sub e 1 (String.length e - 1)) else int_of_string_opt (String.sub e 1 (String.length e - 1)) in
              match code with Some c when c < 128 -> Buffer.add_char b (Char.chr c) | _ -> Buffer.add_string b ("&" ^ e ^ ";"))
          | e -> Buffer.add_string b ("&" ^ e ^ ";"));
          go (j + 1)
      | None -> Buffer.add_char b '&'; go (i + 1))
    else (Buffer.add_char b s.[i]; go (i + 1))
  in
  go 0;
  Buffer.contents b

(* Every <tag>...</tag> text in order. ListObjectsV2 has no nesting of a
   tag inside itself, so a flat scan is enough. *)
let elements tag xml =
  let open_t = "<" ^ tag ^ ">" and close_t = "</" ^ tag ^ ">" in
  let ol = String.length open_t in
  let rec find_from s sub i =
    let n = String.length s and m = String.length sub in
    if i + m > n then None else if String.sub s i m = sub then Some i else find_from s sub (i + 1)
  in
  let rec go i acc =
    match find_from xml open_t i with
    | None -> List.rev acc
    | Some a -> (
        match find_from xml close_t (a + ol) with
        | None -> List.rev acc
        | Some b -> go (b + String.length close_t) (xml_unescape (String.sub xml (a + ol) (b - a - ol)) :: acc))
  in
  go 0 []

type list_page = { keys : string list; next : string option }

let parse_list xml =
  let truncated = elements "IsTruncated" xml = [ "true" ] in
  { keys = elements "Key" xml; next = (if truncated then (match elements "NextContinuationToken" xml with t :: _ -> Some t | [] -> None) else None) }
