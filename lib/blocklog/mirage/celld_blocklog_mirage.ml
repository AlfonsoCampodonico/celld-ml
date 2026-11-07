(* A crash-safe record log on a Mirage block device. Records are appended
   to the active half; [rewrite] writes a replacement set of records to the
   other half and then flips the superblock, so a crash at any point leaves
   one complete half to replay. Callers serialize every operation. *)

open Lwt.Infix
module F = Celld_blocklog

module Make (B : Mirage_block.S) = struct
  type t = {
    blk : B.t;
    sector : int;
    half_sectors : int;
    mutable super : F.super;
    mutable tail : int;  (** bytes used in the active half *)
    mutable tail_sector : Bytes.t;  (** contents of the sector holding [tail] *)
  }

  exception Full
  exception Io of string

  let capacity t = t.half_sectors * t.sector
  let used t = t.tail
  let base_sector t half = 2 + (half * t.half_sectors)

  let read t ~sector ~nsec =
    let cs = Cstruct.create (nsec * t.sector) in
    B.read t.blk (Int64.of_int sector) [ cs ] >>= function
    | Error e -> Lwt.fail (Io (Fmt.str "%a" B.pp_error e))
    | Ok () -> Lwt.return (Cstruct.to_string cs)

  let write t ~sector buf =
    B.write t.blk (Int64.of_int sector) [ Cstruct.of_bytes buf ] >>= function
    | Error e -> Lwt.fail (Io (Fmt.str "%a" B.pp_write_error e))
    | Ok () -> Lwt.return_unit

  (* [len] bytes at byte [off] of [half]. *)
  let read_at t half off len =
    let first = off / t.sector and last = (off + len - 1) / t.sector in
    read t ~sector:(base_sector t half + first) ~nsec:(last - first + 1) >|= fun s ->
    String.sub s (off - (first * t.sector)) len

  let open_ blk =
    B.get_info blk >>= fun info ->
    let sector = info.sector_size in
    let half_sectors = (Int64.to_int info.size_sectors - 2) / 2 in
    if half_sectors < 8 then Lwt.fail (Io "log device too small")
    else
      let t = { blk; sector; half_sectors; super = { gen = 0L; half = 0 }; tail = 0; tail_sector = Bytes.make sector '\000' } in
      read t ~sector:0 ~nsec:2 >>= fun s ->
      t.super <- F.pick_super (String.sub s 0 sector) (String.sub s sector sector);
      let cap = capacity t and half = t.super.half and gen = t.super.gen in
      let rec go off acc =
        if off + F.header_len > cap then Lwt.return (off, acc)
        else
          read_at t half off F.header_len >>= fun h ->
          match F.record_len ~gen ~room:(cap - off) h with
          | None -> Lwt.return (off, acc)
          | Some len -> (
              read_at t half off len >>= fun r ->
              match F.record_payload r with None -> Lwt.return (off, acc) | Some p -> go (off + len) (p :: acc))
      in
      go 0 [] >>= fun (tail, rev) ->
      let tsec = tail / sector in
      (if tail mod sector = 0 || tsec >= half_sectors then Lwt.return (Bytes.make sector '\000')
       else
         read t ~sector:(base_sector t half + tsec) ~nsec:1 >|= fun s ->
         (* Whatever follows the tail is a torn or stale write: zero it. *)
         let b = Bytes.of_string s in
         Bytes.fill b (tail mod sector) (sector - (tail mod sector)) '\000';
         b)
      >|= fun ts ->
      t.tail <- tail;
      t.tail_sector <- ts;
      (t, List.rev rev)

  let append t payload =
    let r = F.encode_record ~gen:t.super.gen payload in
    let len = String.length r in
    if t.tail + len > capacity t then Lwt.fail Full
    else
      let first = t.tail / t.sector in
      let in_first = t.tail - (first * t.sector) in
      let nsec = (in_first + len + t.sector - 1) / t.sector in
      let buf = Bytes.make (nsec * t.sector) '\000' in
      Bytes.blit t.tail_sector 0 buf 0 in_first;
      Bytes.blit_string r 0 buf in_first len;
      write t ~sector:(base_sector t t.super.half + first) buf >|= fun () ->
      let tail = t.tail + len in
      let idx = (tail / t.sector) - first in
      t.tail_sector <- (if idx < nsec then Bytes.sub buf (idx * t.sector) t.sector else Bytes.make t.sector '\000');
      t.tail <- tail

  let rewrite t payloads =
    let super = { F.gen = Int64.succ t.super.gen; half = 1 - t.super.half } in
    let body = String.concat "" (List.map (F.encode_record ~gen:super.gen) payloads) in
    let len = String.length body in
    if len > capacity t then Lwt.fail Full
    else
      let nsec = max 1 ((len + t.sector - 1) / t.sector) in
      let buf = Bytes.make (nsec * t.sector) '\000' in
      Bytes.blit_string body 0 buf 0 len;
      write t ~sector:(base_sector t super.half) buf >>= fun () ->
      let sb = Bytes.make t.sector '\000' in
      Bytes.blit_string (F.encode_super super) 0 sb 0 F.super_len;
      write t ~sector:(Int64.to_int (Int64.rem super.gen 2L)) sb >|= fun () ->
      t.super <- super;
      t.tail <- len;
      let tsec = len / t.sector in
      t.tail_sector <- (if tsec < nsec then Bytes.sub buf (tsec * t.sector) t.sector else Bytes.make t.sector '\000')
end
