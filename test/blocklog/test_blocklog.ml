open Celld_blocklog

let failures = ref 0
let checks = ref 0

let check name ok =
  incr checks;
  if not ok then (incr failures; Printf.printf "FAIL %s\n" name)

let () =
  let s = { gen = 7L; half = 1 } in
  check "super roundtrip" (decode_super (encode_super s) = Some s);
  let torn = Bytes.of_string (encode_super s) in
  Bytes.set torn 10 'x';
  check "torn super rejected" (decode_super (Bytes.to_string torn) = None);
  check "blank disk" (pick_super (String.make 512 '\000') (String.make 512 '\000') = { gen = 0L; half = 0 });
  check "newest wins" (pick_super (encode_super { gen = 4L; half = 0 }) (encode_super { gen = 5L; half = 1 }) = { gen = 5L; half = 1 });
  check "valid beats torn"
    (pick_super (encode_super { gen = 4L; half = 0 }) (Bytes.to_string torn) = { gen = 4L; half = 0 });
  let r = encode_record ~gen:3L "hello" in
  check "record len" (record_len ~gen:3L ~room:100 r = Some (String.length r));
  check "record payload" (record_payload r = Some "hello");
  check "stale gen ends replay" (record_len ~gen:4L ~room:100 r = None);
  check "record must fit" (record_len ~gen:3L ~room:(String.length r - 1) r = None);
  let bad = Bytes.of_string r in
  Bytes.set bad (String.length r - 1) '!';
  check "corrupt payload" (record_payload (Bytes.to_string bad) = None);
  check "zeros are not a record" (record_len ~gen:0L ~room:100 (String.make 16 '\000') = None);
  check "empty payload" (record_payload (encode_record ~gen:0L "") = Some "");
  if !failures > 0 then (Printf.printf "test_blocklog: %d/%d failed\n" !failures !checks; exit 1)
  else Printf.printf "test_blocklog: %d checks passed\n" !checks
