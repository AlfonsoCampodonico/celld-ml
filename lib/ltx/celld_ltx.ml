(* LTX v3 / ltx v0.5.2, following celld's reference/ltx-format.md. *)

let magic = "LTX1"
let header_size = 100
let page_header_size = 6
let trailer_size = 16
let header_flag_no_checksum = 1 lsl 1
let header_flag_mask = header_flag_no_checksum
let page_header_flag_size = 1 lsl 0
let page_header_flag_mask = page_header_flag_size
let pending_byte = 0x4000_0000

let is_valid_page_size n =
  n >= 512 && n <= 65536 && n land (n - 1) = 0

let lock_pgno page_size = if page_size = 0 then 0 else (pending_byte / page_size) + 1

(* ── CRC-64/GO-ISO ───────────────────────────────────────────────────────── *)

let checksum_flag = Int64.min_int

let crc_table =
  let poly = 0xD800000000000000L in
  Array.init 256 (fun i ->
      let c = ref (Int64.of_int i) in
      for _ = 1 to 8 do
        if Int64.logand !c 1L <> 0L then
          c := Int64.logxor (Int64.shift_right_logical !c 1) poly
        else c := Int64.shift_right_logical !c 1
      done;
      !c)

let crc64_sub crc s off len =
  let c = ref (Int64.lognot crc) in
  for i = off to off + len - 1 do
    let idx = (Int64.to_int !c lxor Char.code (String.unsafe_get s i)) land 0xff in
    c := Int64.logxor crc_table.(idx) (Int64.shift_right_logical !c 8)
  done;
  Int64.lognot !c

let crc64 ?(crc = 0L) s = crc64_sub crc s 0 (String.length s)

let be32 n =
  let b = Bytes.create 4 in
  Bytes.set_int32_be b 0 (Int32.of_int (n land 0xffff_ffff));
  Bytes.unsafe_to_string b

let page_checksum ~pgno data =
  Int64.logor checksum_flag (crc64 ~crc:(crc64 (be32 pgno)) data)

let xor_pages acc pages =
  List.fold_left (fun acc (pgno, data) -> Int64.logxor acc (page_checksum ~pgno data)) acc pages

let db_checksum pages = Int64.logor checksum_flag (xor_pages checksum_flag pages)

let update_db_checksum c ~removed ~added =
  Int64.logor checksum_flag (xor_pages (xor_pages c removed) added)

(* ── Header ──────────────────────────────────────────────────────────────── *)

type header = {
  flags : int;
  page_size : int;
  commit : int;
  min_txid : int64;
  max_txid : int64;
  timestamp : int64;
  pre_apply_checksum : int64;
  wal_offset : int64;
  wal_size : int64;
  wal_salt1 : int32;
  wal_salt2 : int32;
  node_id : int64;
}

let default_header ~page_size ~commit ~min_txid ~max_txid =
  {
    flags = 0;
    page_size;
    commit;
    min_txid;
    max_txid;
    timestamp = 0L;
    pre_apply_checksum = 0L;
    wal_offset = 0L;
    wal_size = 0L;
    wal_salt1 = 0l;
    wal_salt2 = 0l;
    node_id = 0L;
  }

let is_snapshot h = h.min_txid = 1L
let no_checksum h = h.flags land header_flag_no_checksum <> 0
let has_flag c = Int64.logand c checksum_flag <> 0L
let is_u32 n = n >= 0 && n <= 0xffff_ffff

let validate_header h =
  let err e = Error e in
  if not (is_u32 h.flags) || h.flags land lnot header_flag_mask <> 0 then err "invalid flags"
  else if not (is_valid_page_size h.page_size) then err "invalid page size"
  else if not (is_u32 h.commit) then err "invalid commit"
  else if h.min_txid = 0L then err "minimum transaction id required"
  else if h.max_txid = 0L then err "maximum transaction id required"
  else if Int64.unsigned_compare h.min_txid h.max_txid > 0 then err "transaction ids out of order"
  else if Int64.compare h.wal_offset 0L < 0 then err "wal offset cannot be negative"
  else if Int64.compare h.wal_size 0L < 0 then err "wal size cannot be negative"
  else if (h.wal_salt1 <> 0l || h.wal_salt2 <> 0l) && h.wal_offset = 0L then
    err "wal offset required if salt exists"
  else if h.wal_offset = 0L && h.wal_size <> 0L then err "wal offset required if wal size exists"
  else if is_snapshot h then
    if h.pre_apply_checksum <> 0L then err "pre-apply checksum must be zero on snapshots"
    else Ok ()
  else if no_checksum h then
    if h.pre_apply_checksum <> 0L then err "pre-apply checksum not allowed" else Ok ()
  else if h.pre_apply_checksum = 0L then err "pre-apply checksum required on non-snapshot files"
  else if not (has_flag h.pre_apply_checksum) then err "invalid pre-apply checksum format"
  else Ok ()

let set_u32 b off n = Bytes.set_int32_be b off (Int32.of_int (n land 0xffff_ffff))
let get_u32 s off = Int32.to_int (String.get_int32_be s off) land 0xffff_ffff

let encode_header h =
  let b = Bytes.make header_size '\000' in
  Bytes.blit_string magic 0 b 0 4;
  set_u32 b 4 h.flags;
  set_u32 b 8 h.page_size;
  set_u32 b 12 h.commit;
  Bytes.set_int64_be b 16 h.min_txid;
  Bytes.set_int64_be b 24 h.max_txid;
  Bytes.set_int64_be b 32 h.timestamp;
  Bytes.set_int64_be b 40 h.pre_apply_checksum;
  Bytes.set_int64_be b 48 h.wal_offset;
  Bytes.set_int64_be b 56 h.wal_size;
  Bytes.set_int32_be b 64 h.wal_salt1;
  Bytes.set_int32_be b 68 h.wal_salt2;
  Bytes.set_int64_be b 72 h.node_id;
  Bytes.unsafe_to_string b

let ( let* ) = Result.bind

let decode_header s =
  if String.length s < header_size then Error "short header"
  else if String.sub s 0 4 <> magic then Error "bad magic"
  else
    let h =
      {
        flags = get_u32 s 4;
        page_size = get_u32 s 8;
        commit = get_u32 s 12;
        min_txid = String.get_int64_be s 16;
        max_txid = String.get_int64_be s 24;
        timestamp = String.get_int64_be s 32;
        pre_apply_checksum = String.get_int64_be s 40;
        wal_offset = String.get_int64_be s 48;
        wal_size = String.get_int64_be s 56;
        wal_salt1 = String.get_int32_be s 64;
        wal_salt2 = String.get_int32_be s 68;
        node_id = String.get_int64_be s 72;
      }
    in
    let* () = validate_header h in
    Ok h

(* ── LZ4 block ───────────────────────────────────────────────────────────── *)

(* An LZ4 block codec written from the public block format description
   (lz4_Block_format.md). A block is a run of sequences: a token (literal
   length in the high nibble, match length - 4 in the low nibble, 15 meaning
   "more length bytes follow"), the literals, a 2-byte little-endian offset
   and the extra match length. The last sequence has literals only. Encoders
   must leave the last 5 bytes as literals and start no match within the
   last 12 bytes. *)
module Lz4 = struct
  let min_match = 4
  let last_literals = 5
  let match_limit = 12
  let max_offset = 0xffff
  let hash_bits = 14
  let compress_bound n = n + (n / 255) + 16

  (* Multiplicative (Fibonacci) hash of the 4 bytes at [i]. *)
  let hash4 s i =
    let v = Int32.to_int (String.get_int32_le s i) land 0xffff_ffff in
    ((v * 0x9e37_79b1) land 0xffff_ffff) lsr (32 - hash_bits)

  (* Greedy single-probe matcher: each position remembers where its 4-byte
     prefix was last seen; a hit is extended both ways. Misses advance
     faster the longer the current literal run gets. *)
  let compress src =
    let n = String.length src in
    let out = Bytes.create (compress_bound n) in
    let o = ref 0 in
    let put b =
      Bytes.unsafe_set out !o (Char.unsafe_chr b);
      incr o
    in
    let put_extra len =
      (* [len] is what is left after the nibble's 15. *)
      let len = ref len in
      while !len >= 255 do
        put 255;
        len := !len - 255
      done;
      put !len
    in
    let sequence ~lit_from ~lit_len ~matched =
      let lit_nib = min lit_len 15 in
      let m_nib = match matched with None -> 0 | Some (_, len) -> min (len - min_match) 15 in
      put ((lit_nib lsl 4) lor m_nib);
      if lit_len >= 15 then put_extra (lit_len - 15);
      Bytes.blit_string src lit_from out !o lit_len;
      o := !o + lit_len;
      match matched with
      | None -> ()
      | Some (offset, len) ->
          put (offset land 0xff);
          put (offset lsr 8);
          if len - min_match >= 15 then put_extra (len - min_match - 15)
    in
    let anchor = ref 0 in
    if n > match_limit then begin
      let seen = Array.make (1 lsl hash_bits) (-1) in
      let last_start = n - match_limit and match_end = n - last_literals in
      let i = ref 0 in
      while !i <= last_start do
        let h = hash4 src !i in
        let cand = seen.(h) in
        seen.(h) <- !i;
        if cand >= 0 && !i - cand <= max_offset
           && String.get_int32_le src cand = String.get_int32_le src !i
        then begin
          let start = ref !i and from = ref cand in
          while !start > !anchor && !from > 0 && src.[!start - 1] = src.[!from - 1] do
            decr start;
            decr from
          done;
          let stop = ref (!i + min_match) in
          while !stop < match_end && src.[!stop] = src.[!stop - (!i - cand)] do
            incr stop
          done;
          sequence ~lit_from:!anchor ~lit_len:(!start - !anchor)
            ~matched:(Some (!i - cand, !stop - !start));
          anchor := !stop;
          i := !stop;
          if !i - 2 >= 0 && !i - 2 <= last_start then seen.(hash4 src (!i - 2)) <- !i - 2
        end
        else i := !i + 1 + ((!i - !anchor) lsr 6)
      done
    end;
    sequence ~lit_from:!anchor ~lit_len:(n - !anchor) ~matched:None;
    Bytes.sub_string out 0 !o

  exception Malformed of string

  let decompress src ~max_len =
    let n = String.length src in
    let out = Bytes.create (max 0 max_len) in
    let o = ref 0 and p = ref 0 in
    let fail m = raise (Malformed ("lz4: " ^ m)) in
    let byte () =
      if !p >= n then fail "truncated input";
      let b = Char.code src.[!p] in
      incr p;
      b
    in
    let length nibble =
      let rec more acc =
        if acc > max_len + 0xffff then fail "length overflow";
        let b = byte () in
        if b = 255 then more (acc + 255) else acc + b
      in
      if nibble = 15 then more 15 else nibble
    in
    try
      if n = 0 then fail "empty block";
      let rec loop () =
        let token = byte () in
        let lit = length (token lsr 4) in
        if !p + lit > n then fail "literals past end of input";
        if !o + lit > max_len then fail "output exceeds max_len";
        Bytes.blit_string src !p out !o lit;
        p := !p + lit;
        o := !o + lit;
        if !p < n then begin
          let lo = byte () in
          let offset = lo lor (byte () lsl 8) in
          if offset = 0 then fail "zero offset";
          if offset > !o then fail "offset before start of output";
          let len = length (token land 15) + min_match in
          if !o + len > max_len then fail "output exceeds max_len";
          (* Byte by byte, so an offset shorter than the length repeats. *)
          for k = 0 to len - 1 do
            Bytes.unsafe_set out (!o + k) (Bytes.unsafe_get out (!o - offset + k))
          done;
          o := !o + len;
          loop ()
        end
      in
      loop ();
      Ok (Bytes.sub_string out 0 !o)
    with Malformed m -> Error m
end

(* ── Files ───────────────────────────────────────────────────────────────── *)

type file = {
  header : header;
  pages : (int * string) list;
  post_apply_checksum : int64;
  file_checksum : int64;
}

let write_uvarint buf v =
  let v = ref v in
  while !v >= 0x80 do
    Buffer.add_char buf (Char.unsafe_chr (!v land 0x7f lor 0x80));
    v := !v lsr 7
  done;
  Buffer.add_char buf (Char.unsafe_chr !v)

let validate_trailer h ~post_apply_checksum ~file_checksum =
  let* () =
    if no_checksum h then
      if post_apply_checksum <> 0L then Error "post-apply checksum not allowed" else Ok ()
    else if post_apply_checksum = 0L || not (has_flag post_apply_checksum) then
      Error "invalid post-apply checksum"
    else Ok ()
  in
  if file_checksum = 0L || not (has_flag file_checksum) then Error "invalid file checksum"
  else Ok ()

let encode h ~pages ~post_apply_checksum =
  let invalid fmt = Printf.ksprintf invalid_arg ("Celld_ltx.encode: " ^^ fmt) in
  (match validate_header h with Ok () -> () | Error e -> invalid "%s" e);
  let buf = Buffer.create (header_size + (List.length pages * (h.page_size / 2 + 16)) + 64) in
  let crc = ref 0L in
  let hashed s =
    Buffer.add_string buf s;
    crc := crc64 ~crc:!crc s
  in
  hashed (encode_header h);
  let lock = lock_pgno h.page_size in
  let index = Buffer.create 64 in
  let prev =
    List.fold_left
      (fun prev (pgno, data) ->
        if pgno <= 0 || not (is_u32 pgno) then invalid "page number %d out of range" pgno;
        if pgno > h.commit then invalid "page %d beyond commit %d" pgno h.commit;
        if String.length data <> h.page_size then
          invalid "page %d is %d bytes, page size is %d" pgno (String.length data) h.page_size;
        if pgno = lock then invalid "page %d is the lock page" pgno;
        if is_snapshot h then begin
          if prev = 0 && pgno <> 1 then invalid "snapshot must start at page 1";
          let expected = if prev = lock - 1 then prev + 2 else prev + 1 in
          if prev <> 0 && pgno <> expected then invalid "snapshot pages must be contiguous"
        end
        else if prev >= pgno then invalid "pages must be strictly ascending";
        let offset = Buffer.length buf in
        let compressed = Lz4.compress data in
        let ph = Bytes.create (page_header_size + 4) in
        set_u32 ph 0 pgno;
        Bytes.set_uint16_be ph 4 page_header_flag_size;
        set_u32 ph 6 (String.length compressed);
        hashed (Bytes.unsafe_to_string ph);
        Buffer.add_string buf compressed;
        crc := crc64 ~crc:!crc data;
        write_uvarint index pgno;
        write_uvarint index offset;
        write_uvarint index (Buffer.length buf - offset);
        pgno)
      0 pages
  in
  ignore prev;
  hashed (String.make page_header_size '\000');
  write_uvarint index 0;
  hashed (Buffer.contents index);
  let b8 = Bytes.create 8 in
  Bytes.set_int64_be b8 0 (Int64.of_int (Buffer.length index));
  hashed (Bytes.to_string b8);
  Bytes.set_int64_be b8 0 post_apply_checksum;
  hashed (Bytes.to_string b8);
  let file_checksum = Int64.logor checksum_flag !crc in
  (match validate_trailer h ~post_apply_checksum ~file_checksum with
  | Ok () -> ()
  | Error e -> invalid "%s" e);
  if h.commit = 0 && post_apply_checksum <> checksum_flag then
    invalid "empty database must have post-apply checksum = checksum_flag";
  Bytes.set_int64_be b8 0 file_checksum;
  Buffer.add_bytes buf b8;
  Buffer.contents buf

exception Corrupt of string

let corrupt e = raise (Corrupt e)

let read_uvarint s pos limit =
  let rec go acc shift =
    if !pos >= limit then corrupt "truncated page index";
    let b = Char.code s.[!pos] in
    incr pos;
    if shift = 56 && b > 0x3f then corrupt "page index varint overflow";
    let acc = acc lor ((b land 0x7f) lsl shift) in
    if b < 0x80 then acc else go acc (shift + 7)
  in
  go 0 0

let decode s =
  try
    let len = String.length s in
    let h = match decode_header s with Ok h -> h | Error e -> corrupt e in
    let crc = ref (crc64_sub 0L s 0 header_size) in
    let pos = ref header_size in
    let rolling = ref (if no_checksum h then 0L else checksum_flag) in
    let lock = lock_pgno h.page_size in
    let bound = Lz4.compress_bound h.page_size in
    let rec pages acc =
      if !pos + page_header_size > len then corrupt "short page header";
      let pgno = get_u32 s !pos and flags = String.get_uint16_be s (!pos + 4) in
      crc := crc64_sub !crc s !pos page_header_size;
      pos := !pos + page_header_size;
      if pgno = 0 && flags = 0 then List.rev acc
      else begin
        if pgno = 0 then corrupt "page number required";
        if flags land lnot page_header_flag_mask <> 0 then corrupt "invalid page header flags";
        if flags land page_header_flag_size = 0 then corrupt "legacy LZ4-frame pages unsupported";
        if !pos + 4 > len then corrupt "short page size";
        let csize = get_u32 s !pos in
        crc := crc64_sub !crc s !pos 4;
        pos := !pos + 4;
        if csize > bound then corrupt "compressed page too large";
        if !pos + csize > len then corrupt "truncated page";
        let data =
          match Lz4.decompress (String.sub s !pos csize) ~max_len:h.page_size with
          | Ok d when String.length d = h.page_size -> d
          | Ok _ -> corrupt "page decompressed to wrong size"
          | Error e -> corrupt e
        in
        pos := !pos + csize;
        crc := crc64 ~crc:!crc data;
        if is_snapshot h && (not (no_checksum h)) && pgno <> lock then
          rolling := Int64.logor checksum_flag (Int64.logxor !rolling (page_checksum ~pgno data));
        pages ((pgno, data) :: acc)
      end
    in
    let pages = pages [] in
    let remaining = len - !pos in
    if remaining < 8 + trailer_size then corrupt "short trailer";
    let trailer_off = len - trailer_size in
    let size_off = trailer_off - 8 in
    let index_size = String.get_int64_be s size_off in
    if index_size <> Int64.of_int (size_off - !pos) then corrupt "page index size mismatch";
    let ip = ref !pos in
    let rec index () = if read_uvarint s ip size_off <> 0 then begin
        ignore (read_uvarint s ip size_off);
        ignore (read_uvarint s ip size_off);
        index ()
      end
    in
    index ();
    if !ip <> size_off then corrupt "trailing bytes in page index";
    let post_apply_checksum = String.get_int64_be s trailer_off in
    let file_checksum = String.get_int64_be s (trailer_off + 8) in
    (match validate_trailer h ~post_apply_checksum ~file_checksum with
    | Ok () -> ()
    | Error e -> corrupt e);
    crc := crc64_sub !crc s !pos (len - 8 - !pos);
    if Int64.logor checksum_flag !crc <> file_checksum then corrupt "file checksum mismatch";
    if is_snapshot h && (not (no_checksum h)) && !rolling <> post_apply_checksum then
      corrupt "post-apply checksum mismatch";
    Ok { header = h; pages; post_apply_checksum; file_checksum }
  with Corrupt e -> Error e

(* ── Keys ────────────────────────────────────────────────────────────────── *)

let filename ~min_txid ~max_txid = Printf.sprintf "%016Lx-%016Lx.ltx" min_txid max_txid

let key ~epoch ~level ~min_txid ~max_txid =
  Printf.sprintf "e%016x/%04x/%s" epoch level (filename ~min_txid ~max_txid)

let hex_ok s = s <> "" && String.for_all (function '0' .. '9' | 'a' .. 'f' -> true | _ -> false) s
let hex64 s = if String.length s = 16 && hex_ok s then Int64.of_string_opt ("0x" ^ s) else None

let parse_key k =
  (* e(16)/(4)/(16)-(16).ltx = 1+16+1+4+1+16+1+16+4 = 60 *)
  if String.length k <> 60 then None
  else if k.[0] <> 'e' || k.[17] <> '/' || k.[22] <> '/' || k.[39] <> '-'
          || String.sub k 56 4 <> ".ltx"
  then None
  else
    let lvl = String.sub k 18 4 in
    match (hex64 (String.sub k 1 16), hex64 (String.sub k 23 16), hex64 (String.sub k 40 16)) with
    | Some e, Some mn, Some mx when hex_ok lvl && Int64.compare e 0L >= 0
                                    && Int64.compare e (Int64.of_int max_int) <= 0 ->
        Some (Int64.to_int e, int_of_string ("0x" ^ lvl), mn, mx)
    | _ -> None
