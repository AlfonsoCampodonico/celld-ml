(* LTX v3 / ltx v0.5.2, ported from celld's crates/ltx (ltx.rs, codec.rs,
   lz4_block.rs) and reference/ltx-format.md. *)

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

module Lz4 = struct
  let min_match = 4
  let window_size = 1 lsl 16
  let window_mask = window_size - 1
  let hash_log = 16
  let hash_size = 1 lsl hash_log
  let match_find_limit = 10 + min_match
  let compress_bound n = n + (n / 255) + 16
  let prime_6_bytes = 227_718_039_650_203L

  let block_hash v =
    Int64.to_int
      (Int64.shift_right_logical (Int64.mul (Int64.shift_left v 16) prime_6_bytes) (64 - hash_log))

  (* Port of pierrec/lz4 v4.1.23 CompressBlock (via celld's lz4_block.rs).
     The hash table holds 16-bit positions; [in_use] marks live slots. *)
  let compress src =
    let n = String.length src in
    let table = Bytes.create (2 * hash_size) in
    let in_use = Bytes.make (hash_size / 8) '\000' in
    let get hash si =
      let h = hash land (hash_size - 1) in
      let pos =
        if Char.code (Bytes.unsafe_get in_use (h lsr 3)) land (1 lsl (h land 7)) <> 0 then
          Bytes.get_uint16_le table (2 * h)
        else 0
      in
      let pos = pos + (si land lnot window_mask) in
      if pos >= si then pos - window_size else pos
    in
    let put hash si =
      let h = hash land (hash_size - 1) in
      Bytes.set_uint16_le table (2 * h) (si land 0xffff);
      let i = h lsr 3 in
      Bytes.unsafe_set in_use i
        (Char.unsafe_chr (Char.code (Bytes.unsafe_get in_use i) lor (1 lsl (h land 7))))
    in
    let matches_at value reference offset =
      offset > 0 && offset < window_size && reference >= 0
      && Int64.to_int32 value = String.get_int32_le src reference
    in
    let dst = Bytes.create (compress_bound n) in
    let di = ref 0 in
    let byte v =
      Bytes.unsafe_set dst !di (Char.unsafe_chr v);
      incr di
    in
    let write_length len =
      let len = ref len in
      while !len >= 0xff do
        byte 0xff;
        len := !len - 0xff
      done;
      byte !len
    in
    let si = ref 0 and anchor = ref 0 in
    let sn = if n > match_find_limit then n - match_find_limit else 0 in
    (if n > match_find_limit then
       try
         while !si < sn do
           let matched = String.get_int64_le src !si in
           let hash = block_hash matched in
           let hash2 = block_hash (Int64.shift_right_logical matched 8) in
           let reference = ref (get hash !si) in
           let reference2 = get hash2 (!si + 1) in
           put hash !si;
           put hash2 (!si + 1);
           let offset = ref (!si - !reference) in
           let found =
             if matches_at matched !reference !offset then true
             else begin
               let hash3 = block_hash (Int64.shift_right_logical matched 16) in
               let reference3 = get hash3 (!si + 2) in
               incr si;
               reference := reference2;
               offset := !si - !reference;
               if matches_at (Int64.shift_right_logical matched 8) !reference !offset then true
               else begin
                 incr si;
                 reference := reference3;
                 offset := !si - !reference;
                 put hash3 !si;
                 if matches_at (Int64.shift_right_logical matched 16) !reference !offset then true
                 else begin
                   si := !si + 2 + ((!si - !anchor) lsr 7);
                   false
                 end
               end
             end
           in
           if found then begin
             let offset = !offset in
             let literal_len = ref (!si - !anchor) in
             let match_len = ref min_match in
             let target = ref (!si - offset - 1) in
             while
               !literal_len > 0 && !target >= 0
               && String.unsafe_get src (!si - 1) = String.unsafe_get src !target
             do
               decr si;
               decr target;
               decr literal_len;
               incr match_len
             done;
             let match_base = !si + min_match in
             si := !si + !match_len;
             (try
                while !si + 8 <= sn do
                  let k = ref 0 in
                  while
                    !k < 8 && String.unsafe_get src (!si + !k) = String.unsafe_get src (!si - offset + !k)
                  do
                    incr k
                  done;
                  si := !si + !k;
                  if !k < 8 then raise Exit
                done
              with Exit -> ());
             let match_len = !si - match_base in
             let literal_len = !literal_len in
             let token_offset = !di in
             byte (min match_len 0x0f);
             if literal_len < 0x0f then
               Bytes.unsafe_set dst token_offset
                 (Char.unsafe_chr (Char.code (Bytes.get dst token_offset) lor (literal_len lsl 4)))
             else begin
               Bytes.unsafe_set dst token_offset
                 (Char.unsafe_chr (Char.code (Bytes.get dst token_offset) lor 0xf0));
               write_length (literal_len - 0x0f)
             end;
             Bytes.blit_string src !anchor dst !di literal_len;
             di := !di + literal_len;
             Bytes.set_uint16_le dst !di offset;
             di := !di + 2;
             anchor := !si;
             if match_len >= 0x0f then write_length (match_len - 0x0f);
             if !si >= sn then raise Exit;
             put (block_hash (String.get_int64_le src (!si - 2))) (!si - 2)
           end
         done
       with Exit -> ());
    let literal_len = n - !anchor in
    if literal_len < 0x0f then byte (literal_len lsl 4)
    else begin
      byte 0xf0;
      let l = ref literal_len in
      while !l >= 0xff + 0x0f do
        byte 0xff;
        l := !l - 0xff
      done;
      byte (!l - 0x0f)
    end;
    Bytes.blit_string src !anchor dst !di literal_len;
    di := !di + literal_len;
    Bytes.sub_string dst 0 !di

  exception Bad of string

  let decompress src ~max_len =
    let n = String.length src in
    let out = Bytes.create (max 0 max_len) in
    let op = ref 0 and ip = ref 0 in
    let next () =
      if !ip >= n then raise (Bad "lz4: truncated input");
      let c = Char.code (String.unsafe_get src !ip) in
      incr ip;
      c
    in
    let ext_len base =
      if base < 15 then base
      else begin
        let len = ref base and continue = ref true in
        while !continue do
          let b = next () in
          len := !len + b;
          if !len > max_len + 0xffff then raise (Bad "lz4: length overflow");
          if b <> 0xff then continue := false
        done;
        !len
      end
    in
    try
      if n = 0 then raise (Bad "lz4: empty block");
      let finished = ref false in
      while not !finished do
        let token = next () in
        let lit = ext_len (token lsr 4) in
        if !ip + lit > n then raise (Bad "lz4: literals past end of input");
        if !op + lit > max_len then raise (Bad "lz4: output exceeds max_len");
        Bytes.blit_string src !ip out !op lit;
        ip := !ip + lit;
        op := !op + lit;
        if !ip = n then finished := true
        else begin
          let lo = next () in
          let hi = next () in
          let offset = lo lor (hi lsl 8) in
          if offset = 0 then raise (Bad "lz4: zero offset");
          if offset > !op then raise (Bad "lz4: offset before start of output");
          let mlen = ext_len (token land 0x0f) + min_match in
          if !op + mlen > max_len then raise (Bad "lz4: output exceeds max_len");
          let from = !op - offset in
          if offset >= mlen then Bytes.blit out from out !op mlen
          else
            for i = 0 to mlen - 1 do
              Bytes.unsafe_set out (!op + i) (Bytes.unsafe_get out (from + i))
            done;
          op := !op + mlen
        end
      done;
      Ok (Bytes.sub_string out 0 !op)
    with Bad e -> Error e
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
