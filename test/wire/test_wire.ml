open Celld_logic
open Celld_wire

let check name c = if not c then (Printf.printf "FAIL %s\n" name; exit 1) else Printf.printf "ok   %s\n" name

let () =
  let l = { l_node = "n1"; l_session = "s\"x"; l_addr = "10.0.0.11:8081"; l_expires_ms = 1761264000123; l_protocol = 5;
            l_log = Log_open; l_followers = [ { p_node = "n2"; p_addr = "10.0.0.12:8081" } ]; l_etag = "" } in
  check "lease json round trip" (Wire.lease_of_json (Wire.lease_to_json l) ~etag:"e9" = Ok { l with l_etag = "e9" });
  check "owner json round trip" (Wire.owner_of_json (Wire.owner_to_json ~node:(Some "n1") ~epoch:7) ~etag:"x" = Ok { o_node = Some "n1"; o_epoch = 7; o_etag = "x" });
  check "released owner" (match Wire.owner_of_json "{\"node\":\"\",\"epoch\":3}" ~etag:"" with Ok r -> r.o_node = None && r.o_epoch = 3 | _ -> false);
  let b = { Wire.leader = "sess"; truncate_to = 4;
            entries = [ { seq = 5; cell = "Counter:x"; cell_epoch = 2; txid = 9L; ltx = "LTX1\000\001" };
                        { seq = 6; cell = "Kv:y"; cell_epoch = 1; txid = 1L; ltx = String.make 300 'z' } ] } in
  check "batch round trip" (Wire.decode_batch (Wire.encode_batch b) = Ok b);
  check "batch rejects garbage" (Result.is_error (Wire.decode_batch "CLA1\255"));
  check "json rejects garbage" (Result.is_error (Wire.of_string "{\"a\":"));
  check "constant-time equal" (Wire.equal_ct "abc" "abc" && not (Wire.equal_ct "abc" "abd") && not (Wire.equal_ct "a" "ab"))
