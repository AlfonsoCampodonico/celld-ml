(* A cell's state: an ordered KV map, serialized into fixed-size pages so that
   every commit can be captured as an LTX file, the way celld captures SQLite
   WAL frames. The page image is a pure function of the map, so restore only
   has to replay pages. *)

module M = Map.Make (String)

let page_size = 4096
let magic = "CKV1"

type t = {
  mutable kv : string M.t;
  mutable txid : int64;  (** last committed transaction *)
  mutable pages : string array;  (** page [i] is pgno [i+1] *)
  mutable checksum : int64;  (** rolling database checksum of [pages] *)
}

let empty () = { kv = M.empty; txid = 0L; pages = [||]; checksum = Celld_ltx.db_checksum [] }

let be32 n =
  let b = Bytes.create 4 in
  Bytes.set_int32_be b 0 (Int32.of_int n);
  Bytes.unsafe_to_string b

let serialize kv =
  let buf = Buffer.create 256 in
  M.iter
    (fun k v ->
      Buffer.add_string buf (be32 (String.length k));
      Buffer.add_string buf k;
      Buffer.add_string buf (be32 (String.length v));
      Buffer.add_string buf v)
    kv;
  let payload = Buffer.contents buf in
  magic ^ be32 (String.length payload) ^ payload

let paginate image =
  let n = max 1 ((String.length image + page_size - 1) / page_size) in
  Array.init n (fun i ->
      let off = i * page_size in
      let len = min page_size (String.length image - off) in
      let p = Bytes.make page_size '\000' in
      if len > 0 then Bytes.blit_string image off p 0 len;
      Bytes.unsafe_to_string p)

let deserialize pages =
  let image = String.concat "" (Array.to_list pages) in
  if String.length image < 8 || String.sub image 0 4 <> magic then Error "bad page image"
  else
    let total = Int32.to_int (String.get_int32_be image 4) in
    let rec go off stop acc =
      if off >= stop then Ok acc
      else
        let kl = Int32.to_int (String.get_int32_be image off) in
        let k = String.sub image (off + 4) kl in
        let vl = Int32.to_int (String.get_int32_be image (off + 4 + kl)) in
        let v = String.sub image (off + 8 + kl) vl in
        go (off + 8 + kl + vl) stop (M.add k v acc)
    in
    try go 8 (8 + total) M.empty with Invalid_argument _ -> Error "truncated page image"

let indexed pages = Array.to_list (Array.mapi (fun i p -> (i + 1, p)) pages)

(* An LTX file covering every page: MinTXID = 1 makes it a snapshot. *)
let snapshot t =
  let h = Celld_ltx.default_header ~page_size ~commit:(Array.length t.pages) ~min_txid:1L ~max_txid:t.txid in
  Celld_ltx.encode h ~pages:(indexed t.pages) ~post_apply_checksum:t.checksum

(* Commit the current [kv]: returns the new txid and its LTX file. The first
   transaction of a database is necessarily a snapshot. *)
let commit t =
  let pages = paginate (serialize t.kv) in
  let txid = Int64.succ t.txid in
  let changed = ref [] and removed = ref [] in
  Array.iteri
    (fun i p ->
      if i >= Array.length t.pages then changed := (i + 1, p) :: !changed
      else if t.pages.(i) <> p then begin
        changed := (i + 1, p) :: !changed;
        removed := (i + 1, t.pages.(i)) :: !removed
      end)
    pages;
  for i = Array.length pages to Array.length t.pages - 1 do
    removed := (i + 1, t.pages.(i)) :: !removed
  done;
  let checksum = Celld_ltx.update_db_checksum t.checksum ~removed:!removed ~added:!changed in
  let pre = t.checksum in
  t.pages <- pages;
  t.txid <- txid;
  t.checksum <- checksum;
  let file =
    if txid = 1L then snapshot t
    else
      let h = Celld_ltx.default_header ~page_size ~commit:(Array.length pages) ~min_txid:txid ~max_txid:txid in
      Celld_ltx.encode { h with pre_apply_checksum = pre } ~pages:(List.rev !changed) ~post_apply_checksum:checksum
  in
  (txid, file)

(* Apply a restore chain: a snapshot followed by contiguous incrementals. A
   file that does not continue the chain ends it. *)
let restore files =
  let t = empty () in
  let apply (f : Celld_ltx.file) =
    let h = f.header in
    let snap = Celld_ltx.is_snapshot h in
    if (not snap) && (h.min_txid <> Int64.succ t.txid || h.pre_apply_checksum <> t.checksum) then false
    else begin
      let n = h.commit in
      let pages = Array.make n (String.make page_size '\000') in
      if not snap then Array.blit t.pages 0 pages 0 (min n (Array.length t.pages));
      List.iter (fun (pgno, data) -> if pgno <= n then pages.(pgno - 1) <- data) f.pages;
      t.pages <- pages;
      t.txid <- h.max_txid;
      t.checksum <- f.post_apply_checksum;
      true
    end
  in
  let rec go = function [] -> () | f :: rest -> if apply f then go rest in
  go files;
  if t.txid = 0L then Ok t
  else match deserialize t.pages with Ok kv -> t.kv <- kv; Ok t | Error e -> Error e

(* Choose the restore chain from the keys under cells/<cell>/ltx/: the newest
   epoch that opens with a snapshot, then that epoch's files that continue it.
   A stale owner can only have written into a superseded epoch prefix, so the
   epoch is the fence. *)
let plan keys =
  let parsed = List.filter_map (fun k -> Option.map (fun p -> (k, p)) (Celld_ltx.parse_key k)) keys in
  let epochs = List.sort_uniq (fun a b -> compare b a) (List.map (fun (_, (e, _, _, _)) -> e) parsed) in
  let rec pick = function
    | [] -> []
    | e :: older -> (
        let files = List.filter (fun (_, (e', _, _, _)) -> e' = e) parsed in
        let snaps = List.filter (fun (_, (_, _, mn, _)) -> mn = 1L) files in
        match List.sort (fun (_, (_, _, _, a)) (_, (_, _, _, b)) -> compare b a) snaps with
        | [] -> pick older
        | (snap_key, (_, _, _, top)) :: _ ->
            let rec chain cur acc =
              match List.find_opt (fun (_, (_, _, mn, _)) -> mn = Int64.succ cur) files with
              | Some (k, (_, _, _, mx)) -> chain mx (k :: acc)
              | None -> List.rev acc
            in
            snap_key :: chain top [])
  in
  pick epochs

(* Keys under cells/<cell>/ltx/ that a resident owner at [epoch] may delete
   once a snapshot at txid [upto] of that epoch is in the bucket ([upto] = 0
   when only the epoch's opening snapshot is known to be there):
   - every file of an older epoch, since restore never reads past the newest
     epoch that has a snapshot;
   - files of this epoch that end at or below [upto], except the snapshot
     ending exactly at [upto] that the next restore starts from. *)
let garbage keys ~epoch ~upto =
  List.filter
    (fun k ->
      match Celld_ltx.parse_key k with
      | None -> false
      | Some (e, _, mn, mx) ->
          e < epoch || (e = epoch && upto > 0 && Int64.compare mx (Int64.of_int upto) <= 0 && not (mn = 1L && mx = Int64.of_int upto)))
    keys
