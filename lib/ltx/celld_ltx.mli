(** LTX (Lite Transaction) file format, version 3 / ltx v0.5.2 layout.

    Pure OCaml, stdlib only (no Unix, no C stubs), so it builds for Solo5.
    All multi-byte integers are big-endian. Unsigned 64-bit quantities
    (TXIDs, checksums, node ids) are carried in [int64] with their bit
    pattern preserved; compare them with [Int64.unsigned_compare]. *)

(** {1 Constants} *)

val magic : string
(** ["LTX1"]. *)

val header_size : int
(** 100. *)

val page_header_size : int
(** 6. *)

val trailer_size : int
(** 16. *)

val header_flag_no_checksum : int
(** [1 lsl 1], the only valid header flag. *)

val page_header_flag_size : int
(** [1 lsl 0]: a 4-byte compressed size and a raw LZ4 block follow the
    page header. Every page this library writes carries it. *)

val is_valid_page_size : int -> bool
(** Power of two in [512, 65536]. *)

val lock_pgno : int -> int
(** SQLite lock page for a page size: [0x40000000 / page_size + 1]; 0 for a
    zero page size. *)

(** {1 Checksums} *)

val checksum_flag : int64
(** [1 lsl 63], OR-ed into every stored checksum. *)

val crc64 : ?crc:int64 -> string -> int64
(** CRC-64/GO-ISO (Go [crc64.ISO] table: reflected poly 0xD800000000000000,
    init 0, invert-process-invert per update). Chainable:
    [crc64 ~crc:(crc64 a) b = crc64 (a ^ b)]. *)

val page_checksum : pgno:int -> string -> int64
(** [checksum_flag | crc64 (BE_u32 pgno ^ data)]. *)

val db_checksum : (int * string) list -> int64
(** [checksum_flag | xor of page checksums]; [checksum_flag] for no pages.
    The caller must leave out the SQLite lock page (as the reference does). *)

val update_db_checksum :
  int64 -> removed:(int * string) list -> added:(int * string) list -> int64
(** Incremental form of {!db_checksum}: xor out the [removed] pages' old
    contents and xor in the [added] pages' new contents. *)

(** {1 Header} *)

type header = {
  flags : int;
  page_size : int;
  commit : int;  (** DB size in pages after this transaction. *)
  min_txid : int64;
  max_txid : int64;
  timestamp : int64;  (** ms since unix epoch *)
  pre_apply_checksum : int64;  (** 0 on snapshots *)
  wal_offset : int64;
  wal_size : int64;
  wal_salt1 : int32;
  wal_salt2 : int32;
  node_id : int64;
}

val default_header :
  page_size:int -> commit:int -> min_txid:int64 -> max_txid:int64 -> header
(** All other fields zero. A non-snapshot header still needs a
    [pre_apply_checksum] before it validates. *)

val is_snapshot : header -> bool
(** [min_txid = 1]. *)

val validate_header : header -> (unit, string) result
(** The reference [Header::validate] invariants. *)

val encode_header : header -> string
(** The 100-byte header. *)

val decode_header : string -> (header, string) result
(** Parses and validates the first 100 bytes. *)

(** {1 Files} *)

type file = {
  header : header;
  pages : (int * string) list;  (** in file order, decompressed *)
  post_apply_checksum : int64;
  file_checksum : int64;
}

val encode : header -> pages:(int * string) list -> post_apply_checksum:int64 -> string
(** Encodes a v0.5.2 file (sized raw LZ4 blocks, byte-exact with upstream's
    pierrec/lz4 fast compressor). Raises [Invalid_argument] when the header
    is invalid, a page is not [page_size] long, pages are not strictly
    ascending (snapshots: contiguous from 1, skipping the lock page), a page
    is the lock page or above [commit], or the post-apply checksum is
    malformed for the header's checksum mode. *)

val decode : string -> (file, string) result
(** Decodes and fully verifies: magic, header invariants, page headers,
    compressed sizes, page index framing, trailer, the file checksum and,
    for snapshots with checksums, the rolling post-apply checksum.
    Only the v0.5.2 sized-block page representation is accepted; legacy
    LZ4-frame pages are rejected. *)

(** {1 Object keys} *)

val filename : min_txid:int64 -> max_txid:int64 -> string
(** ["<min:016x>-<max:016x>.ltx"]. *)

val key : epoch:int -> level:int -> min_txid:int64 -> max_txid:int64 -> string
(** ["e<epoch:016x>/<level:04x>/<min:016x>-<max:016x>.ltx"], relative to
    [cells/<cell>/ltx/]. *)

val parse_key : string -> (int * int * int64 * int64) option
(** Inverse of {!key}: [(epoch, level, min_txid, max_txid)]. Lowercase hex
    with exact widths only. *)

(** {1 LZ4 block format} *)

module Lz4 : sig
  val compress_bound : int -> int
  (** [n + n / 255 + 16]. *)

  val compress : string -> string
  (** One independent LZ4 block, a port of pierrec/lz4 v4.1.23's fast block
      compressor (byte-identical to what upstream LTX writes). *)

  val decompress : string -> max_len:int -> (string, string) result
  (** Full LZ4 block decoder; errors when the output would exceed
      [max_len] or the block is malformed. *)
end
