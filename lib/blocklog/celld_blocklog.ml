(* On-disk framing for a crash-safe record log split into two halves.

   Sectors 0 and 1 hold superblock copies; the one with a valid CRC and the
   highest generation names the active half. Records in a half carry the
   low 32 bits of that generation, so stale records left over from an older
   use of the same half end replay instead of being read back. All integers
   are big-endian.

     superblock: "CBS1" | crc32 (bytes 8..17) | gen u64 | half u8
     record:     "CLR1" | crc32 (bytes 8..len) | gen u32 | len u32 | payload *)

let crc_table =
  Array.init 256 (fun n ->
      let c = ref (Int32.of_int n) in
      for _ = 0 to 7 do
        if Int32.logand !c 1l <> 0l then c := Int32.logxor 0xEDB88320l (Int32.shift_right_logical !c 1)
        else c := Int32.shift_right_logical !c 1
      done;
      !c)

let crc32 s off len =
  let c = ref 0xFFFFFFFFl in
  for i = off to off + len - 1 do
    let idx = Int32.to_int (Int32.logand (Int32.logxor !c (Int32.of_int (Char.code (String.unsafe_get s i)))) 0xFFl) in
    c := Int32.logxor crc_table.(idx) (Int32.shift_right_logical !c 8)
  done;
  Int32.logxor !c 0xFFFFFFFFl

let super_magic = "CBS1"
let super_len = 17
let record_magic = "CLR1"
let header_len = 16

type super = { gen : int64; half : int }

let encode_super { gen; half } =
  let b = Bytes.make super_len '\000' in
  Bytes.blit_string super_magic 0 b 0 4;
  Bytes.set_int64_be b 8 gen;
  Bytes.set_uint8 b 16 half;
  Bytes.set_int32_be b 4 (crc32 (Bytes.unsafe_to_string b) 8 (super_len - 8));
  Bytes.to_string b

let decode_super s =
  if String.length s < super_len || String.sub s 0 4 <> super_magic then None
  else if crc32 s 8 (super_len - 8) <> String.get_int32_be s 4 then None
  else
    let half = String.get_uint8 s 16 in
    if half > 1 then None else Some { gen = String.get_int64_be s 8; half }

(* The newest valid copy; a blank disk is generation 0 on half 0. *)
let pick_super a b =
  match decode_super a, decode_super b with
  | Some x, Some y -> if Int64.compare x.gen y.gen >= 0 then x else y
  | Some x, None | None, Some x -> x
  | None, None -> { gen = 0L; half = 0 }

let gen32 gen = Int64.to_int32 gen

let encode_record ~gen payload =
  let len = header_len + String.length payload in
  let b = Bytes.create len in
  Bytes.blit_string record_magic 0 b 0 4;
  Bytes.set_int32_be b 8 (gen32 gen);
  Bytes.set_int32_be b 12 (Int32.of_int len);
  Bytes.blit_string payload 0 b header_len (String.length payload);
  Bytes.set_int32_be b 4 (crc32 (Bytes.unsafe_to_string b) 8 (len - 8));
  Bytes.to_string b

(* From a record header: [Some len] when the record belongs to [gen] and
   fits in [room] bytes. *)
let record_len ~gen ~room h =
  if String.length h < header_len || String.sub h 0 4 <> record_magic then None
  else if String.get_int32_be h 8 <> gen32 gen then None
  else
    let len = Int32.to_int (String.get_int32_be h 12) land 0xffff_ffff in
    if len < header_len || len > room then None else Some len

let record_payload r =
  if crc32 r 8 (String.length r - 8) <> String.get_int32_be r 4 then None
  else Some (String.sub r header_len (String.length r - header_len))
