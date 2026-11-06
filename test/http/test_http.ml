open Celld_http

let check name b = if not b then failwith ("FAIL: " ^ name)

let done_req = function Done (r, n) -> (r, n) | Partial -> failwith "partial" | Error e -> failwith e
let done_resp = function Done (r, n) -> (r, n) | Partial -> failwith "partial" | Error e -> failwith e

let () =
  (* header lookup is case-insensitive *)
  let hs = [ ("Content-Type", "text/plain"); ("ETAG", "\"x\"") ] in
  check "header ci" (header "content-type" hs = Some "text/plain");
  check "header ci 2" (header "ETag" hs = Some "\"x\"");
  check "header missing" (header "host" hs = None);

  (* simple request with body *)
  let raw = "PUT /o/a%2Fb HTTP/1.1\r\nHost: x\r\ncontent-length: 5\r\nIf-None-Match: *\r\n\r\nhello" in
  let r, n = done_req (parse_request raw) in
  check "meth" (r.meth = "PUT");
  check "target" (r.target = "/o/a%2Fb");
  check "body" (r.body = "hello");
  check "consumed" (n = String.length raw);
  check "inm" (header "if-none-match" r.headers = Some "*");

  (* every strict prefix is Partial *)
  for i = 0 to String.length raw - 1 do
    match parse_request (String.sub raw 0 i) with
    | Partial -> ()
    | _ -> failwith (Printf.sprintf "prefix %d not partial" i)
  done;

  (* pipelining: two requests back to back *)
  let r2 = "GET /list?prefix=a%20b&x=1+2 HTTP/1.1\r\nHost: x\r\n\r\n" in
  let buf = raw ^ r2 ^ "GET /o/" in
  let _, n1 = done_req (parse_request buf) in
  check "pipe offset 1" (n1 = String.length raw);
  let rest = String.sub buf n1 (String.length buf - n1) in
  let q, n2 = done_req (parse_request rest) in
  check "pipe offset 2" (n2 = String.length r2);
  check "pipe body empty" (q.body = "");
  let rest2 = String.sub rest n2 (String.length rest - n2) in
  check "pipe tail partial" (parse_request rest2 = Partial);
  let path, params = path_and_query q.target in
  check "path" (path = "/list");
  check "query" (params = [ ("prefix", "a b"); ("x", "1 2") ]);

  (* chunked request *)
  let ch =
    "POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5;ext=1\r\nhello\r\n7\r\n, world\r\n0\r\nTrailer: y\r\n\r\nNEXT"
  in
  let r, n = done_req (parse_request ch) in
  check "chunked body" (r.body = "hello, world");
  check "chunked consumed" (n = String.length ch - 4);
  check "te removed" (header "transfer-encoding" r.headers = None);
  for i = 0 to String.length ch - 5 do
    match parse_request (String.sub ch 0 i) with
    | Partial -> ()
    | _ -> failwith (Printf.sprintf "chunked prefix %d not partial" i)
  done;

  (* errors *)
  check "bad line" (match parse_request "GARBAGE\r\n\r\n" with Error _ -> true | _ -> false);
  check "bad cl" (match parse_request "GET / HTTP/1.1\r\nContent-Length: x\r\n\r\n" with Error _ -> true | _ -> false);
  check "bad chunk" (match parse_request "GET / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n" with Error _ -> true | _ -> false);

  (* responses *)
  let resp = { status = 412; headers = [ ("ETag", "\"e1\"") ]; body = "nope" } in
  let s = serialize_response resp in
  check "reason" (String.sub s 0 33 = "HTTP/1.1 412 Precondition Failed\r");
  let p, n = done_resp (parse_response s) in
  check "resp rt status" (p.status = 412);
  check "resp rt body" (p.body = "nope");
  check "resp rt consumed" (n = String.length s);
  check "resp cl" (header "content-length" p.headers = Some "4");
  (* HEAD response: Content-Length but no body *)
  let h = "HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"a\"\r\n\r\n" in
  let p, n = done_resp (parse_response ~head:true h) in
  check "head body" (p.body = "" && n = String.length h);
  check "non-head partial" (parse_response h = Partial);
  (* 204 has no body *)
  let s204 = serialize_response { status = 204; headers = []; body = "" } in
  check "204 no cl" (header "content-length" (fst (done_resp (parse_response s204))).headers = None);
  (* unframed body reads to eof *)
  let u = "HTTP/1.1 200 OK\r\n\r\nabc" in
  check "unframed partial" (parse_response u = Partial);
  check "unframed eof" ((fst (done_resp (parse_response_eof u))).body = "abc");
  (* chunked response *)
  let cr = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n" in
  check "chunked resp" ((fst (done_resp (parse_response cr))).body = "abc");

  (* request round trip; Host and Content-Length added, stale CL replaced *)
  let req = { meth = "PUT"; target = "/o/k"; headers = [ ("Content-Length", "99"); ("If-Match", "\"e\"") ]; body = "data\r\n\r\nmore" } in
  let s = serialize_request req in
  let p, n = done_req (parse_request s) in
  check "req rt" (p.meth = "PUT" && p.target = "/o/k" && p.body = req.body && n = String.length s);
  check "req host" (header "host" p.headers = Some "localhost");
  check "req cl" (header "content-length" p.headers = Some (string_of_int (String.length req.body)));
  check "req if-match" (header "if-match" p.headers = Some "\"e\"");
  let s' = serialize_request { req with headers = [ ("host", "h:9000") ] } in
  check "req host kept" (header "Host" (fst (done_req (parse_request s'))).headers = Some "h:9000");

  (* percent encoding *)
  let key = "cells/Kv:a b/ltx/e0000/x?y&z=%\xff" in
  let enc = pct_encode key in
  check "enc safe" (String.for_all (fun c -> c <> ' ' && c <> '?' && c <> '&' && c <> ':') enc);
  check "enc keeps slash" (String.sub enc 0 6 = "cells/");
  check "pct rt" (pct_decode enc = key);
  check "pct malformed" (pct_decode "a%zz%4" = "a%zz%4");
  check "pct plus kept" (pct_decode "a+b" = "a+b");
  check "pq none" (path_and_query "/o/x" = ("/o/x", []));

  (* expected_length *)
  check "explen" (expected_length "PUT / HTTP/1.1\r\nContent-Length: 10\r\n\r\nab" = Some 48);
  check "explen partial head" (expected_length "PUT / HTTP/1.1\r\n" = None);

  print_endline "test_http: all ok"
