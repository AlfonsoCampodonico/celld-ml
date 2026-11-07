open Celld_ws

let check name c = if not c then (Printf.printf "FAIL %s\n" name; exit 1) else Printf.printf "ok   %s\n" name

let () =
  (* RFC 6455 section 1.3 *)
  check "accept key" (accept_key "dGhlIHNhbXBsZSBub25jZQ==" = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
  (* RFC 6455 section 5.7: a masked "Hello" from a client *)
  let hello = "\x81\x85\x37\xfa\x21\x3d\x7f\x9f\x4d\x51\x58" in
  (match decode hello with `Frame (f, used) -> check "masked text frame" (f.fin && f.opcode = Text && f.payload = "Hello" && used = 11) | _ -> check "masked text frame" false);
  check "partial frame" (decode (String.sub hello 0 5) = `Partial);
  check "unmasked client frame refused" (match decode "\x81\x05Hello" with `Error _ -> true | _ -> false);
  check "server frame (RFC example)" (encode Text "Hello" = "\x81\x05Hello");
  check "16-bit length" (String.length (encode Binary (String.make 256 'x')) = 4 + 256 && (encode Binary (String.make 256 'x')).[1] = '\126');
  check "64-bit length" (String.length (encode Binary (String.make 70000 'x')) = 10 + 70000);
  let mask_frame op payload =
    let m = "\x01\x02\x03\x04" in
    let n = String.length payload in
    let hdr = if n < 126 then String.make 1 (Char.chr (0x80 lor n)) else "\xfe" ^ (let b = Bytes.create 2 in Bytes.set_uint16_be b 0 n; Bytes.to_string b) in
    String.make 1 (Char.chr (0x80 lor op)) ^ hdr ^ m ^ String.init n (fun i -> Char.chr (Char.code payload.[i] lxor Char.code m.[i land 3]))
  in
  (match decode (mask_frame 2 (String.make 300 'z')) with `Frame (f, _) -> check "masked 16-bit binary" (f.payload = String.make 300 'z') | _ -> check "masked 16-bit binary" false);
  (match decode (mask_frame 8 "\x03\xe8bye") with `Frame (f, _) -> check "close frame" (f.opcode = Close && close_info f.payload = (1000, "bye")) | _ -> check "close frame" false);
  check "oversized frame refused" (match decode ~max_payload:10 (mask_frame 2 (String.make 300 'z')) with `Error _ -> true | _ -> false);
  check "close frame encoding" (close_frame 1012 "" = "\x88\x02\x03\xf4")
