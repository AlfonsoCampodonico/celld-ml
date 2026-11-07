open Celld_ltx

let passed = ref 0

let check name cond =
  if not cond then failwith ("FAIL: " ^ name);
  incr passed


let unhex h =
  String.init (String.length h / 2) (fun i -> Char.chr (int_of_string ("0x" ^ String.sub h (2 * i) 2)))

let is_error = function Error _ -> true | Ok _ -> false
let rng = Random.State.make [| 42 |]
let random_string n = String.init n (fun _ -> Char.chr (Random.State.int rng 256))

(* ── CRC64 ── *)
let () =
  check "crc64 check value" (crc64 "123456789" = 0xb90956c775a41001L);
  check "crc64 empty" (crc64 "" = 0L);
  check "crc64 chain" (crc64 ~crc:(crc64 "1234") "56789" = crc64 "123456789");
  check "page_checksum def"
    (page_checksum ~pgno:7 "abc" = Int64.logor checksum_flag (crc64 ("\000\000\000\007abc")));
  check "flag" (checksum_flag = Int64.shift_left 1L 63);
  check "db_checksum empty" (db_checksum [] = checksum_flag);
  let p1 = (1, String.make 512 'a') and p2 = (2, String.make 512 'b') in
  let p2' = (2, String.make 512 'c') and p3 = (3, String.make 512 'd') in
  let c = db_checksum [ p1; p2 ] in
  check "db_checksum has flag" (Int64.logand c checksum_flag <> 0L);
  check "db_checksum order-free" (c = db_checksum [ p2; p1 ]);
  check "update_db_checksum"
    (update_db_checksum c ~removed:[ p2 ] ~added:[ p2'; p3 ] = db_checksum [ p1; p2'; p3 ]);
  check "update noop" (update_db_checksum c ~removed:[] ~added:[] = c)

(* ── LZ4 ── *)
let roundtrip name s =
  let c = Lz4.compress s in
  check (name ^ " bound") (String.length c <= Lz4.compress_bound (String.length s));
  check (name ^ " roundtrip") (Lz4.decompress c ~max_len:(String.length s) = Ok s);
  c

let () =
  ignore (roundtrip "empty" "");
  check "empty block bytes" (Lz4.compress "" = "\000");
  ignore (roundtrip "short" "hello");
  ignore (roundtrip "15 bytes" "abcdefghijklmno");
  for i = 0 to 40 do
    let n = Random.State.int rng 70000 in
    ignore (roundtrip (Printf.sprintf "random %d" i) (random_string n))
  done;
  let z = roundtrip "zeros 4096" (String.make 4096 '\000') in
  check "zeros compress well" (String.length z < 64);
  let z = roundtrip "zeros 65536" (String.make 65536 '\000') in
  check "zeros 64k compress well" (String.length z < 400);
  let text =
    String.concat "" (List.init 300 (fun i -> Printf.sprintf "key-%d=value-%d;" (i mod 17) (i mod 5)))
  in
  let c = roundtrip "text" text in
  check "text compresses" (String.length c * 2 < String.length text);
  (* Mixed: runs of random bytes interleaved with repeats, incl. long lengths. *)
  for i = 0 to 30 do
    let b = Buffer.create 8192 in
    while Buffer.length b < 8192 do
      if Random.State.bool rng then Buffer.add_string b (random_string (Random.State.int rng 300))
      else Buffer.add_string b (String.make (Random.State.int rng 600) (Char.chr (Random.State.int rng 4)))
    done;
    ignore (roundtrip (Printf.sprintf "mixed %d" i) (Buffer.contents b))
  done;
  (* Encoder end-of-block rules: the last 5 bytes are literals, and no
     match starts within the last 12 bytes. *)
  let ends_ok s =
    let c = Lz4.compress s and n = String.length s in
    let p = ref 0 and o = ref 0 and ok = ref true in
    let byte () = let b = Char.code c.[!p] in incr p; b in
    let len nib = if nib < 15 then nib else (let l = ref 15 and go = ref true in
      while !go do let b = byte () in l := !l + b; go := b = 255 done; !l) in
    while !p < String.length c do
      let t = byte () in
      let lit = len (t lsr 4) in
      p := !p + lit; o := !o + lit;
      if !p < String.length c then begin
        p := !p + 2;
        if !o > n - 12 then ok := false;
        o := !o + len (t land 15) + 4;
        if !o > n - 5 then ok := false
      end
    done;
    !ok
  in
  check "end rules zeros" (ends_ok (String.make 4096 '\000'));
  check "end rules text" (ends_ok text);
  for i = 0 to 20 do
    check (Printf.sprintf "end rules random %d" i) (ends_ok (String.make (13 + i) 'q'))
  done;
  (* Hand-made blocks with overlapping match copies. *)
  check "overlap offset 1"
    (Lz4.decompress (unhex "1661010000") ~max_len:100 = Ok (String.make 11 'a'));
  check "overlap offset 3 + final literals"
    (Lz4.decompress (unhex "356162630300" ^ "\x50xyzzy") ~max_len:100 = Ok "abcabcabcabcxyzzy");
  (* Extended match length: literal 'z', offset 1, length 15+4+3 = 22. *)
  check "extended match length"
    (Lz4.decompress (unhex "1f7a01000300") ~max_len:100 = Ok (String.make 23 'z'));
  (* Extended literal length: 15 + 255 + 2 = 272 literals. *)
  let lits = String.make 272 'q' in
  check "extended literal length" (Lz4.decompress ("\xf0\xff\x02" ^ lits) ~max_len:272 = Ok lits);
  check "zero offset" (is_error (Lz4.decompress (unhex "1661000000") ~max_len:100));
  check "offset too far" (is_error (Lz4.decompress (unhex "1661020000") ~max_len:100));
  check "max_len exceeded" (is_error (Lz4.decompress (unhex "1661010000") ~max_len:10));
  check "truncated literals" (is_error (Lz4.decompress "\x50abc" ~max_len:100));
  check "truncated offset" (is_error (Lz4.decompress "\x16a\x01" ~max_len:100));
  check "empty input" (is_error (Lz4.decompress "" ~max_len:100))

(* ── LTX files ── *)
let page_size = 4096
let page i = String.init page_size (fun j -> Char.chr ((i * 31 + j / 7) land 0xff))

let () =
  let pages = List.init 5 (fun i -> (i + 1, page (i + 1))) in
  let h =
    { (default_header ~page_size ~commit:5 ~min_txid:1L ~max_txid:3L) with
      timestamp = 1_700_000_000_000L; node_id = 0xdeadbeefL }
  in
  check "is_snapshot" (is_snapshot h);
  let post = db_checksum pages in
  let bytes = encode h ~pages ~post_apply_checksum:post in
  check "magic" (String.sub bytes 0 4 = "LTX1");
  check "header bytes" (String.sub bytes 0 100 = encode_header h);
  check "first page flag" (String.get_uint16_be bytes 104 = page_header_flag_size);
  check "trailer post" (String.get_int64_be bytes (String.length bytes - 16) = post);
  (match decode bytes with
  | Ok f ->
      check "snap header" (f.header = h);
      check "snap pages" (f.pages = pages);
      check "snap post" (f.post_apply_checksum = post);
      check "snap file checksum flag" (Int64.logand f.file_checksum checksum_flag <> 0L)
  | Error e -> failwith ("snapshot decode: " ^ e));
  (* Tamper with every byte position class: header, page body, index, trailer. *)
  let flip off =
    let b = Bytes.of_string bytes in
    Bytes.set b off (Char.chr (Char.code (Bytes.get b off) lxor 0x01));
    Bytes.to_string b
  in
  let len = String.length bytes in
  List.iter
    (fun off -> check (Printf.sprintf "tamper @%d" off) (is_error (decode (flip off))))
    [ 0; 20; 33; 79; 100; 103; 112; 200; len - 30; len - 17; len - 9; len - 1 ];
  check "truncated" (is_error (decode (String.sub bytes 0 (len - 1))));
  check "extended" (is_error (decode (bytes ^ "\000")));
  (* A snapshot whose post-apply checksum is wrong but file checksum is right. *)
  let bad = encode h ~pages ~post_apply_checksum:(Int64.logor checksum_flag 12345L) in
  check "snapshot post-apply mismatch" (decode bad = Error "post-apply checksum mismatch");
  (* Incremental file. *)
  let ipages = [ (2, page 20); (4, page 40); (9, page 90) ] in
  let ih =
    { (default_header ~page_size ~commit:9 ~min_txid:4L ~max_txid:4L) with pre_apply_checksum = post }
  in
  check "not snapshot" (not (is_snapshot ih));
  let ipost =
    update_db_checksum post ~removed:[ (2, page 2); (4, page 4) ] ~added:ipages
  in
  let ibytes = encode ih ~pages:ipages ~post_apply_checksum:ipost in
  (match decode ibytes with
  | Ok f ->
      check "inc header" (f.header = ih);
      check "inc pages" (f.pages = ipages);
      check "inc post" (f.post_apply_checksum = ipost)
  | Error e -> failwith ("incremental decode: " ^ e));
  (* Incremental files don't verify post-apply against pages. *)
  check "inc arbitrary post ok"
    (Result.is_ok (decode (encode ih ~pages:ipages ~post_apply_checksum:(Int64.logor checksum_flag 7L))));
  (* Empty-db snapshot: commit 0, no pages, post = flag. *)
  let eh = default_header ~page_size:512 ~commit:0 ~min_txid:1L ~max_txid:1L in
  let ebytes = encode eh ~pages:[] ~post_apply_checksum:checksum_flag in
  check "empty size" (String.length ebytes = 100 + 6 + 1 + 8 + 16);
  check "empty decode" (match decode ebytes with Ok f -> f.pages = [] | Error _ -> false);
  (* Encoder argument validation. *)
  let raises f = match f () with _ -> false | exception Invalid_argument _ -> true in
  check "unsorted" (raises (fun () -> encode ih ~pages:[ (4, page 4); (2, page 2) ] ~post_apply_checksum:ipost));
  check "dup" (raises (fun () -> encode ih ~pages:[ (2, page 4); (2, page 2) ] ~post_apply_checksum:ipost));
  check "short page" (raises (fun () -> encode ih ~pages:[ (2, "x") ] ~post_apply_checksum:ipost));
  check "beyond commit" (raises (fun () -> encode ih ~pages:[ (10, page 1) ] ~post_apply_checksum:ipost));
  check "snapshot gap" (raises (fun () -> encode h ~pages:[ (1, page 1); (3, page 3) ] ~post_apply_checksum:post));
  check "snapshot not from 1" (raises (fun () -> encode h ~pages:[ (2, page 1) ] ~post_apply_checksum:post));
  check "no pre-apply" (raises (fun () -> encode { ih with pre_apply_checksum = 0L } ~pages:ipages ~post_apply_checksum:ipost));
  check "bad page size" (raises (fun () -> encode { h with page_size = 1000 } ~pages:[] ~post_apply_checksum:post));
  check "post without flag" (raises (fun () -> encode ih ~pages:ipages ~post_apply_checksum:5L));
  check "empty db post" (raises (fun () -> encode eh ~pages:[] ~post_apply_checksum:(Int64.logor checksum_flag 1L)));
  check "lock page" (lock_pgno 4096 = 262145);
  (* No-checksum mode. *)
  let nh = { ih with flags = header_flag_no_checksum; pre_apply_checksum = 0L } in
  check "no-checksum roundtrip"
    (match decode (encode nh ~pages:ipages ~post_apply_checksum:0L) with
     | Ok f -> f.pages = ipages && f.header = nh
     | Error _ -> false);
  (* Large pages of random (incompressible) data. *)
  let rpages = List.init 3 (fun i -> (i + 1, random_string 65536)) in
  let rh = default_header ~page_size:65536 ~commit:3 ~min_txid:1L ~max_txid:1L in
  check "random 64k snapshot"
    (match decode (encode rh ~pages:rpages ~post_apply_checksum:(db_checksum rpages)) with
     | Ok f -> f.pages = rpages
     | Error _ -> false);
  check "bad magic" (decode ("LTX2" ^ String.sub bytes 4 (len - 4)) = Error "bad magic")

(* ── Keys ── *)
let () =
  let k = key ~epoch:3 ~level:0 ~min_txid:1L ~max_txid:0x2aL in
  check "key format" (k = "e0000000000000003/0000/0000000000000001-000000000000002a.ltx");
  check "parse_key" (parse_key k = Some (3, 0, 1L, 0x2aL));
  check "filename" (filename ~min_txid:1L ~max_txid:1L = "0000000000000001-0000000000000001.ltx");
  let k2 = key ~epoch:max_int ~level:0xffff ~min_txid:(-1L) ~max_txid:Int64.min_int in
  check "parse_key extremes" (parse_key k2 = Some (max_int, 0xffff, -1L, Int64.min_int));
  check "uppercase rejected" (parse_key (String.uppercase_ascii k) = None);
  check "bad ext" (parse_key (String.sub k 0 56 ^ ".lt0") = None);
  check "short" (parse_key "e0/0000/1-2.ltx" = None);
  check "epoch overflow"
    (parse_key "effffffffffffffff/0000/0000000000000001-0000000000000001.ltx" = None)

let () = Printf.printf "test_ltx: %d checks passed\n" !passed
