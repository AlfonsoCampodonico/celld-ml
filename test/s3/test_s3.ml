open Celld_s3

let check name c = if not c then (Printf.printf "FAIL %s\n" name; exit 1) else Printf.printf "ok   %s\n" name

let () =
  (* AWS's SigV4 documentation, "Example: GET Object" (header-based auth). *)
  let creds = { access_key = "AKIAIOSFODNN7EXAMPLE"; secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"; region = "us-east-1" } in
  let hs =
    sign creds ~amz_date:"20130524T000000Z" ~meth:"GET" ~host:"examplebucket.s3.amazonaws.com" ~path:"/test.txt" ~query:[]
      ~headers:[ ("Range", "bytes=0-9") ] ~payload_sha256:empty_sha256
  in
  check "AWS GET Object example signature"
    (List.assoc "Authorization" hs
    = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41");
  (* "Example: GET Bucket (List Objects)": query parameters are signed. *)
  let hs =
    sign creds ~amz_date:"20130524T000000Z" ~meth:"GET" ~host:"examplebucket.s3.amazonaws.com" ~path:"/" ~query:[ ("max-keys", "2"); ("prefix", "J") ]
      ~headers:[] ~payload_sha256:empty_sha256
  in
  check "AWS list example signature"
    (String.ends_with ~suffix:"Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7" (List.assoc "Authorization" hs));
  check "key encoding" (uri_encode ~slash:true "cells/Counter:a b/own.json" = "cells/Counter%3Aa%20b/own.json");
  check "amz date" (amz_date_of_unix 1369353600 = "20130524T000000Z" && amz_date_of_unix 1793964000 = "20261106T112000Z");
  let page =
    parse_list
      {|<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>b</Name><KeyCount>2</KeyCount><IsTruncated>true</IsTruncated><Contents><Key>nodes/n1.json</Key><ETag>&quot;abc&quot;</ETag></Contents><Contents><Key>a&amp;b</Key></Contents><NextContinuationToken>tok/=</NextContinuationToken></ListBucketResult>|}
  in
  check "list keys" (page.keys = [ "nodes/n1.json"; "a&b" ]);
  check "list continuation" (page.next = Some "tok/=");
  check "last page" ((parse_list "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>").next = None)
