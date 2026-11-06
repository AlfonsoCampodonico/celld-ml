(* The bucket: an S3-ish object store with conditional writes, backed by an
   append-only record log on a block device. See DESIGN.md. *)

open Lwt.Infix

let src = Logs.Src.create "bucket" ~doc:"celld bucket"

module Log = (val Logs.src_log src : Logs.LOG)

let put_delay_ms =
  let doc =
    Cmdliner.Arg.info ~doc:"Sleep this many ms before acknowledging a PUT or DELETE."
      [ "put-delay-ms" ]
  in
  Mirage_runtime.register_arg Cmdliner.Arg.(value & opt int 0 doc)

let port = 9000

(* CRC-32 (IEEE). *)
let crc_table =
  Array.init 256 (fun n ->
      let c = ref (Int32.of_int n) in
      for _ = 0 to 7 do
        if Int32.logand !c 1l <> 0l then
          c := Int32.logxor 0xEDB88320l (Int32.shift_right_logical !c 1)
        else c := Int32.shift_right_logical !c 1
      done;
      !c)

let crc32 (b : Bytes.t) off len =
  let c = ref 0xFFFFFFFFl in
  for i = off to off + len - 1 do
    let idx = Int32.to_int (Int32.logand (Int32.logxor !c (Int32.of_int (Char.code (Bytes.get b i)))) 0xFFl) in
    c := Int32.logxor crc_table.(idx) (Int32.shift_right_logical !c 8)
  done;
  Int32.logxor !c 0xFFFFFFFFl

(* Record layout, all integers big-endian:
     "CBR1" | crc32 (of everything after it) | kind ('P' | 'D')
     | key len u32 | etag len u32 | body len u32 | key | etag | body
   A tombstone ('D') has an empty etag and body. Replay stops at the first
   record whose magic, lengths or CRC do not check out: a fresh all-zeros
   disk is an empty log. *)
let magic = "CBR1"
let header_len = 4 + 4 + 1 + 4 + 4 + 4

let encode_record kind key etag body =
  let kl = String.length key and el = String.length etag and bl = String.length body in
  let len = header_len + kl + el + bl in
  let b = Bytes.create len in
  Bytes.blit_string magic 0 b 0 4;
  Bytes.set b 8 kind;
  Bytes.set_int32_be b 9 (Int32.of_int kl);
  Bytes.set_int32_be b 13 (Int32.of_int el);
  Bytes.set_int32_be b 17 (Int32.of_int bl);
  Bytes.blit_string key 0 b header_len kl;
  Bytes.blit_string etag 0 b (header_len + kl) el;
  Bytes.blit_string body 0 b (header_len + kl + el) bl;
  Bytes.set_int32_be b 4 (crc32 b 8 (len - 8));
  b

module Make (S : Tcpip.Stack.V4V6) (B : Mirage_block.S) = struct
  module H = Celld_http_mirage.Make (S)
  module M = Map.Make (String)

  type disk = {
    blk : B.t;
    sector : int;
    capacity : int; (* bytes *)
    mutable tail : int; (* byte offset of the next record *)
    mutable tail_sector : Bytes.t; (* current contents of the sector holding [tail] *)
  }

  type store = {
    disk : disk;
    mutable objects : (string * string) M.t; (* key -> (body, etag) *)
    lock : Lwt_mutex.t;
  }

  exception Disk_full
  exception Io of string

  (* Read [len] bytes at byte offset [off] (within capacity). *)
  let read_bytes d off len =
    let first = off / d.sector in
    let last = (off + len - 1) / d.sector in
    let nsec = last - first + 1 in
    let cs = Cstruct.create (nsec * d.sector) in
    B.read d.blk (Int64.of_int first) [ cs ] >>= function
    | Error e -> Lwt.fail (Io (Fmt.str "%a" B.pp_error e))
    | Ok () -> Lwt.return (Cstruct.to_string cs ~off:(off - (first * d.sector)) ~len)

  (* Append [rec_] at the tail. The first sector is rewritten with its old
     prefix, so earlier records in it are preserved byte for byte. *)
  let append d rec_ =
    let len = Bytes.length rec_ in
    if d.tail + len > d.capacity then Lwt.fail Disk_full
    else
      let first = d.tail / d.sector in
      let in_first = d.tail - (first * d.sector) in
      let total = in_first + len in
      let nsec = (total + d.sector - 1) / d.sector in
      let buf = Bytes.make (nsec * d.sector) '\000' in
      Bytes.blit d.tail_sector 0 buf 0 in_first;
      Bytes.blit rec_ 0 buf in_first len;
      B.write d.blk (Int64.of_int first) [ Cstruct.of_bytes buf ] >>= function
      | Error e -> Lwt.fail (Io (Fmt.str "%a" B.pp_write_error e))
      | Ok () ->
          let new_tail = d.tail + len in
          let idx = (new_tail / d.sector) - first in
          d.tail_sector <-
            (if idx < nsec then Bytes.sub buf (idx * d.sector) d.sector
             else Bytes.make d.sector '\000');
          d.tail <- new_tail;
          Lwt.return_unit

  let replay d =
    let objects = ref M.empty and count = ref 0 in
    let rec go off =
      if off + header_len > d.capacity then Lwt.return off
      else
        read_bytes d off header_len >>= fun h ->
        if String.sub h 0 4 <> magic then Lwt.return off
        else
          let kind = h.[8] in
          let get o = Int32.to_int (String.get_int32_be h o) in
          let kl = get 9 and el = get 13 and bl = get 17 in
          if kl < 0 || el < 0 || bl < 0 || (kind <> 'P' && kind <> 'D') then Lwt.return off
          else
            let len = header_len + kl + el + bl in
            if off + len > d.capacity then Lwt.return off
            else
              read_bytes d off len >>= fun r ->
              let rb = Bytes.unsafe_of_string r in
              if crc32 rb 8 (len - 8) <> String.get_int32_be r 4 then (
                Log.warn (fun m -> m "replay: bad CRC at offset %d, truncating log there" off);
                Lwt.return off)
              else
                let key = String.sub r header_len kl in
                let etag = String.sub r (header_len + kl) el in
                let body = String.sub r (header_len + kl + el) bl in
                (match kind with
                | 'P' -> objects := M.add key (body, etag) !objects
                | _ -> objects := M.remove key !objects);
                incr count;
                go (off + len)
    in
    go 0 >>= fun tail ->
    let tsec = tail / d.sector in
    (if tail = 0 then Lwt.return (Bytes.make d.sector '\000')
     else if tsec * d.sector >= d.capacity then Lwt.return (Bytes.make d.sector '\000')
     else read_bytes d (tsec * d.sector) d.sector >|= fun s ->
       (* Bytes past the tail may hold garbage from a torn write: zero them. *)
       let b = Bytes.of_string s in
       Bytes.fill b (tail - (tsec * d.sector)) (d.sector - (tail - (tsec * d.sector))) '\000';
       b)
    >|= fun ts ->
    d.tail <- tail;
    d.tail_sector <- ts;
    Log.info (fun m ->
        m "replayed %d records, %d live objects, log %d/%d bytes" !count (M.cardinal !objects)
          tail d.capacity);
    !objects

  (* ETags: the log offset of the record that wrote the value. Offsets only
     grow, so every acknowledged write gets a fresh one. *)
  let etag_for_offset off = Printf.sprintf "%012x" off

  let text status body =
    { Celld_http.status; headers = [ ("Content-Type", "text/plain") ]; body }

  let quoted e = "\"" ^ e ^ "\""

  (* Parse an If-Match / If-None-Match value into [`Any] or a list of etags. *)
  let parse_tags v =
    let v = String.trim v in
    if v = "*" then `Any
    else
      `Tags
        (String.split_on_char ',' v
        |> List.map (fun t ->
               let t = String.trim t in
               let t =
                 if String.length t > 2 && String.sub t 0 2 = "W/" then String.sub t 2 (String.length t - 2)
                 else t
               in
               let n = String.length t in
               if n >= 2 && t.[0] = '"' && t.[n - 1] = '"' then String.sub t 1 (n - 2) else t))

  (* Evaluate preconditions against the current entry. *)
  let precondition headers current =
    let if_match = Option.map parse_tags (Celld_http.header "if-match" headers) in
    let if_none = Option.map parse_tags (Celld_http.header "if-none-match" headers) in
    let cur_etag = Option.map snd current in
    let ok_match =
      match (if_match, cur_etag) with
      | None, _ -> true
      | Some _, None -> false
      | Some `Any, Some _ -> true
      | Some (`Tags ts), Some e -> List.mem e ts
    in
    let ok_none =
      match (if_none, cur_etag) with
      | None, _ | Some _, None -> true
      | Some `Any, Some _ -> false
      | Some (`Tags ts), Some e -> not (List.mem e ts)
    in
    ok_match && ok_none

  let delay () =
    let ms = put_delay_ms () in
    if ms > 0 then Mirage_sleep.ns (Int64.mul (Int64.of_int ms) 1_000_000L) else Lwt.return_unit

  (* Check + log write + map update, under the store lock. *)
  let commit st kind key body headers =
    Lwt_mutex.with_lock st.lock (fun () ->
        let current = M.find_opt key st.objects in
        if not (precondition headers current) then Lwt.return `Precondition_failed
        else if kind = 'D' && current = None then Lwt.return `Not_found
        else
          let off = st.disk.tail in
          let etag = if kind = 'P' then etag_for_offset off else "" in
          Lwt.catch
            (fun () ->
              append st.disk (encode_record kind key etag (if kind = 'P' then body else ""))
              >|= fun () ->
              (if kind = 'P' then st.objects <- M.add key (body, etag) st.objects
               else st.objects <- M.remove key st.objects);
              `Ok etag)
            (function
              | Disk_full -> Lwt.return `Full
              | Io m -> Lwt.return (`Io m)
              | e -> Lwt.fail e))

  let handle st (req : Celld_http.request) =
    let path, query = Celld_http.path_and_query req.target in
    let meth = String.uppercase_ascii req.meth in
    let resp =
      if String.length path >= 3 && String.sub path 0 3 = "/o/" then
        let key = Celld_http.pct_decode (String.sub path 3 (String.length path - 3)) in
        if key = "" then Lwt.return (text 400 "empty key\n")
        else
          match meth with
          | "GET" | "HEAD" -> (
              match M.find_opt key st.objects with
              | None -> Lwt.return (text 404 "not found\n")
              | Some (body, etag) ->
                  Lwt.return
                    {
                      Celld_http.status = 200;
                      headers = [ ("ETag", quoted etag); ("Content-Type", "application/octet-stream") ];
                      body;
                    })
          | "PUT" -> (
              commit st 'P' key req.body req.headers >>= function
              | `Ok etag ->
                  delay () >|= fun () ->
                  { Celld_http.status = 200; headers = [ ("ETag", quoted etag) ]; body = "" }
              | `Precondition_failed -> Lwt.return (text 412 "precondition failed\n")
              | `Not_found -> Lwt.return (text 404 "not found\n")
              | `Full -> Lwt.return (text 507 "log full\n")
              | `Io m -> Lwt.return (text 500 ("block write failed: " ^ m ^ "\n")))
          | "DELETE" -> (
              commit st 'D' key "" req.headers >>= function
              | `Ok _ -> delay () >|= fun () -> { Celld_http.status = 204; headers = []; body = "" }
              | `Precondition_failed -> Lwt.return (text 412 "precondition failed\n")
              | `Not_found -> Lwt.return (text 404 "not found\n")
              | `Full -> Lwt.return (text 507 "log full\n")
              | `Io m -> Lwt.return (text 500 ("block write failed: " ^ m ^ "\n")))
          | _ -> Lwt.return (text 405 "method not allowed\n")
      else if path = "/list" then
        if meth <> "GET" && meth <> "HEAD" then Lwt.return (text 405 "method not allowed\n")
        else
          let prefix = Option.value ~default:"" (List.assoc_opt "prefix" query) in
          let pl = String.length prefix in
          let keys =
            M.fold
              (fun k _ acc ->
                if String.length k >= pl && String.sub k 0 pl = prefix then k :: acc else acc)
              st.objects []
            |> List.rev
          in
          Lwt.return (text 200 (String.concat "\n" keys))
      else Lwt.return (text 404 "no such route\n")
    in
    resp >|= fun (r : Celld_http.response) ->
    Log.info (fun m ->
        m "%s %s -> %d (%d bytes in, %d out)" meth req.target r.status (String.length req.body)
          (String.length r.body));
    r

  let start s blk =
    B.get_info blk >>= fun info ->
    let capacity = Int64.to_int info.size_sectors * info.sector_size in
    let disk =
      { blk; sector = info.sector_size; capacity; tail = 0; tail_sector = Bytes.make info.sector_size '\000' }
    in
    Log.info (fun m ->
        m "storage: %d sectors of %d bytes, put-delay-ms=%d" (Int64.to_int info.size_sectors)
          info.sector_size (put_delay_ms ()));
    replay disk >>= fun objects ->
    let st = { disk; objects; lock = Lwt_mutex.create () } in
    H.serve s ~port (handle st);
    Log.info (fun m -> m "listening on port %d" port);
    S.listen s
end
