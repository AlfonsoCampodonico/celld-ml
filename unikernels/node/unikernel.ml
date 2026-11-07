(* A celld node: the effect executor around Celld_logic.Core.

   One serial actor is the only caller of [on_event] (celld's crates/celld
   actor). Every effect that does I/O runs as its own Lwt task and reports
   back by pushing a completion event, which carries the op it answers.
   The actor never waits on I/O. *)

open Lwt.Infix
open Celld_logic
module Store = Celld_cell.Store
module Handlers = Celld_cell.Handlers
module Wire = Celld_wire.Wire

let src = Logs.Src.create "celld" ~doc:"celld node"

module Log = (val Logs.src_log src : Logs.LOG)

(* ---- runtime arguments ---- *)

let arg name doc c default =
  Mirage_runtime.register_arg Cmdliner.Arg.(value & opt c default (info ~doc [ name ]))

let node_id = arg "node-id" "This node's id." Cmdliner.Arg.string "node"
let advertise = arg "advertise" "ip:port where peers reach this node's internal listener." Cmdliner.Arg.string "10.0.0.11:8081"
let bucket_addr = arg "bucket" "ip:port of the bucket." Cmdliner.Arg.string "10.0.0.2:9000"
let ttl_ms = arg "ttl-ms" "Node lease lifetime." Cmdliner.Arg.int 10_000
let idle_ms = arg "idle-ms" "Evict a cell idle this long." Cmdliner.Arg.int 30_000
let durability = arg "durability" "fleet or bucket." Cmdliner.Arg.string "fleet"
let max_followers = arg "followers" "Followers to recruit in fleet mode." Cmdliner.Arg.int 2
let compact_every = arg "compact-every" "Upload a level-1 snapshot every this many commits, then delete what it covers." Cmdliner.Arg.int 64
let rebalance = arg "rebalance" "Shed cells to less loaded nodes." Cmdliner.Arg.bool true
let upload_delay_ms = arg "upload-delay-ms" "Delay before each LTX upload (to show the fleet path)." Cmdliner.Arg.int 0

let public_port = 8080
let internal_port = 8081

module Make (S : Tcpip.Stack.V4V6) (Blk : Mirage_block.S) = struct
  module H = Celld_http_mirage.Make (S)
  module B = Celld_bucketc.Make (S)
  module D = Celld_blocklog_mirage.Make (Blk)

  let now_ms () = int_of_float (Ptime.to_float_s (Mirage_ptime.now ()) *. 1000.)
  let sleep_ms ms = Mirage_sleep.ns (Int64.mul (Int64.of_int (max 0 ms)) 1_000_000L)
  let random_hex n = Wire.hex (Mirage_crypto_rng.generate n)

  let parse_addr s =
    match String.rindex_opt s ':' with
    | Some i -> (
        match Ipaddr.of_string (String.sub s 0 i), int_of_string_opt (String.sub s (i + 1) (String.length s - i - 1)) with
        | Ok ip, Some port -> (ip, port)
        | _ -> invalid_arg ("bad address " ^ s))
    | None -> invalid_arg ("bad address " ^ s)

  let fail_of (f : B.failure) : failure = match f with B.Definite s -> Definite s | B.Ambiguous s -> Ambiguous s
  let guard_of : guard -> B.guard = function Absent -> B.Absent | Match e -> B.Match e

  let cas_of = function
    | Ok (`Ok etag) -> Applied etag
    | Ok `Precondition_failed -> Rejected
    | Error f -> Cas_failed (fail_of f)

  (* ---- the node log: captured LTX files, shipped to followers and tiered
     to the bucket (celld's node_log.rs, much reduced) ---- *)

  type entry = {
    e : Wire.entry;
    key : string;
    mutable uploaded : bool;
    mutable acked : string list;  (** followers holding it *)
  }

  type durable_waiter = { w_op : op; w_cell : cell_id; w_epoch : int; w_position : int }

  (* What this node holds for other leaders, keyed by leader session. *)
  type held = { mutable sealed : bool; entries : (int, Wire.entry) Hashtbl.t }

  type runtime = {
    store : Store.t;
    epoch : int;
    mutable snap_txid : int;  (** last snapshot the bucket has *)
    js : Celld_js.t option;  (** the class script's context, for JS classes *)
  }

  type t = {
    stack : S.t;
    bucket : B.t;
    core : Core.t;
    me : string;
    session : string;
    fleet : bool;
    mutable secret : string;
    mailbox : (event -> unit);
    runtimes : (cell_id, runtime) Hashtbl.t;
    log : entry Queue.t;  (** entries not yet uploaded, in seq order *)
    mutable next_seq : int;
    mutable active : peer list;  (** followers recorded in our lease and caught up *)
    mutable want : peer list;
    mutable peers : lease list;  (** live, non-draining leases of other nodes, from the last discovery *)
    mutable last_shed_ms : int;
    alarm_timers : (cell_id, int) Hashtbl.t;  (** the alarm each resident cell's timer waits for *)
    held : (string, held) Hashtbl.t;
    disk : D.t;  (** follower log: what [held] is rebuilt from after a restart *)
    disk_lock : Lwt_mutex.t;
    mutable durable_waiters : durable_waiter list;
    completions : (req, (route, request_error) result Lwt.u) Hashtbl.t;
    releases : (req, (unit, request_error) result Lwt.u) Hashtbl.t;
    mutable next_req : int;
    nonces : (string, int) Hashtbl.t;
  }

  (* ---- peer HTTP, signed with the fleet secret ---- *)

  let sha256_hex s = Digestif.SHA256.(to_hex (digest_string s))

  let sign t ~meth ~path ~body =
    let timestamp = string_of_int (now_ms ()) and nonce = random_hex 12 in
    let body_sha256 = sha256_hex body in
    let msg = Wire.canonical ~meth ~path ~body_sha256 ~source:t.me ~timestamp ~nonce in
    let signature = Digestif.SHA256.(to_hex (hmac_string ~key:t.secret msg)) in
    [
      ("x-cells-peer-source", t.me);
      ("x-cells-peer-timestamp", timestamp);
      ("x-cells-peer-nonce", nonce);
      ("x-cells-peer-body-sha256", body_sha256);
      ("x-cells-peer-signature", signature);
    ]

  let verify t (r : Celld_http.request) =
    let h k = Celld_http.header k r.headers in
    match h "x-cells-peer-source", h "x-cells-peer-timestamp", h "x-cells-peer-nonce", h "x-cells-peer-body-sha256", h "x-cells-peer-signature" with
    | Some source, Some timestamp, Some nonce, Some body_sha256, Some signature ->
        let now = now_ms () in
        let fresh = match int_of_string_opt timestamp with Some ts -> abs (now - ts) <= 30_000 | None -> false in
        Hashtbl.filter_map_inplace (fun _ at -> if now - at > 60_000 then None else Some at) t.nonces;
        let msg = Wire.canonical ~meth:r.meth ~path:r.target ~body_sha256 ~source ~timestamp ~nonce in
        let expected = Digestif.SHA256.(to_hex (hmac_string ~key:t.secret msg)) in
        if not fresh then Error "stale timestamp"
        else if Hashtbl.mem t.nonces nonce then Error "replayed nonce"
        else if not (Wire.equal_ct (sha256_hex r.body) body_sha256) then Error "body digest mismatch"
        else if not (Wire.equal_ct expected signature) then Error "bad signature"
        else (Hashtbl.replace t.nonces nonce now; Ok source)
    | _ -> Error "unsigned"

  let peer_call t addr ?(headers = []) ~meth ~path body =
    let ip, port = parse_addr addr in
    let headers = sign t ~meth ~path ~body @ headers in
    H.request t.stack ~timeout_ns:5_000_000_000L ip port { Celld_http.meth; target = path; headers; body }

  (* ---- durability bookkeeping ---- *)

  (* The highest position of [cell]@[epoch] such that it and every earlier
     capture of that epoch satisfies [ok]. Uploaded entries leave the queue,
     so a cell with nothing pending is covered up to its runtime's txid. *)
  let covered t cell epoch ok =
    let top = match Hashtbl.find_opt t.runtimes cell with Some r when r.epoch = epoch -> Int64.to_int r.store.txid | _ -> 0 in
    let first_gap =
      Queue.fold
        (fun acc en ->
          match acc with
          | Some _ -> acc
          | None -> if en.e.cell = cell && en.e.cell_epoch = epoch && not (ok en) then Some (Int64.to_int en.e.txid) else None)
        None t.log
    in
    match first_gap with Some tx -> tx - 1 | None -> top

  let fleet_ok t en = t.active <> [] && List.for_all (fun p -> List.mem p.p_node en.acked) t.active

  let check_durable t =
    let pending, done_ =
      List.partition
        (fun w ->
          let fleet = if t.fleet then covered t w.w_cell w.w_epoch (fleet_ok t) else 0 in
          let bucket = covered t w.w_cell w.w_epoch (fun en -> en.uploaded) in
          if fleet >= w.w_position then (t.mailbox (Durable_reached { op = w.w_op; result = Ok fleet; source = Fleet }); false)
          else if bucket >= w.w_position then (t.mailbox (Durable_reached { op = w.w_op; result = Ok bucket; source = Bucket }); false)
          else true)
        t.durable_waiters
    in
    ignore done_;
    t.durable_waiters <- pending

  let prune t =
    while (not (Queue.is_empty t.log)) && (Queue.peek t.log).uploaded do
      ignore (Queue.pop t.log)
    done

  let truncate_to t = if Queue.is_empty t.log then t.next_seq else (Queue.peek t.log).e.seq

  let rec upload t en attempt =
    sleep_ms (upload_delay_ms ()) >>= fun () ->
    B.put t.bucket en.key en.e.ltx >>= function
    | Ok (`Ok _) ->
        en.uploaded <- true;
        prune t;
        check_durable t;
        Lwt.return_unit
    | Ok `Precondition_failed | Error _ ->
        if attempt < 5 then sleep_ms (200 * (attempt + 1)) >>= fun () -> upload t en (attempt + 1)
        else (Log.err (fun m -> m "upload %s failed" en.key); Lwt.return_unit)

  let drop_follower t (p : peer) =
    if List.exists (fun q -> q.p_node = p.p_node) t.want then begin
      Log.warn (fun m -> m "follower %s failed; dropping it" p.p_node);
      t.active <- List.filter (fun q -> q.p_node <> p.p_node) t.active;
      t.want <- List.filter (fun q -> q.p_node <> p.p_node) t.want;
      t.mailbox (Followers_chosen t.want);
      check_durable t
    end

  let ship t (p : peer) entries =
    let body = Wire.encode_batch { leader = t.session; truncate_to = truncate_to t; entries = List.map (fun en -> en.e) entries } in
    peer_call t p.p_addr ~meth:"POST" ~path:"/peer/log/append" body >|= function
    | Ok { status = 200; _ } ->
        List.iter (fun en -> if not (List.mem p.p_node en.acked) then en.acked <- p.p_node :: en.acked) entries;
        true
    | _ -> false

  (* Capture one commit: the same bytes go to the followers and the bucket.
     The output gate decides which proof arrives first. *)
  let capture t cell epoch txid ltx =
    match Celld_ltx.decode ltx with
    | Error e -> Log.err (fun m -> m "capture produced a bad LTX file: %s" e)
    | Ok f ->
        let key = Wire.ltx_prefix cell ^ Celld_ltx.key ~epoch ~level:0 ~min_txid:f.header.min_txid ~max_txid:f.header.max_txid in
        let en = { e = { seq = t.next_seq; cell; cell_epoch = epoch; txid; ltx }; key; uploaded = false; acked = [] } in
        t.next_seq <- t.next_seq + 1;
        Queue.push en t.log;
        Lwt.async (fun () -> upload t en 0);
        if t.fleet then
          List.iter
            (fun p ->
              Lwt.async (fun () ->
                  ship t p [ en ] >|= fun ok ->
                  if ok then check_durable t else drop_follower t p))
            t.active

  (* Activate_followers: a new follower first receives the backlog that is not
     yet in the bucket, then counts toward fleet proofs. *)
  let activate t peers =
    t.active <- List.filter (fun a -> List.exists (fun p -> p.p_node = a.p_node) peers) t.active;
    List.iter
      (fun p ->
        if not (List.exists (fun a -> a.p_node = p.p_node) t.active) then
          Lwt.async (fun () ->
              let backlog = List.of_seq (Queue.to_seq t.log) in
              ship t p backlog >|= fun ok ->
              if ok then begin
                Log.info (fun m -> m "follower %s active (backlog %d)" p.p_node (List.length backlog));
                t.active <- p :: t.active;
                check_durable t
              end
              else drop_follower t p))
      peers

  (* Live, compatible, non-draining leases of the other nodes. *)
  let discover t =
    B.list t.bucket ~prefix:"nodes/" >>= function
    | Error _ -> Lwt.return_unit
    | Ok keys ->
        Lwt_list.filter_map_s
          (fun k ->
            B.get t.bucket k >|= function
            | Ok (Some (body, etag)) -> (
                match Wire.lease_of_json body ~etag with
                | Ok l when l.l_node <> t.me && l.l_expires_ms > now_ms () && l.l_protocol = 5 && not l.l_draining -> Some l
                | _ -> None)
            | _ -> None)
          keys
        >|= fun leases -> t.peers <- leases

  (* Recruit followers from the discovered peers. *)
  let recruit t =
    let live = List.map (fun l -> { p_node = l.l_node; p_addr = l.l_addr }) t.peers in
    let keep = List.filter (fun p -> List.exists (fun q -> q.p_node = p.p_node) live) t.want in
    let fresh = List.filter (fun p -> not (List.exists (fun q -> q.p_node = p.p_node) keep)) live in
    let rec take n = function [] -> [] | x :: r -> if n <= 0 then [] else x :: take (n - 1) r in
    let want = take (max_followers ()) (keep @ fresh) in
    if want <> t.want then begin
      Log.info (fun m -> m "followers -> [%s]" (String.concat "," (List.map (fun p -> p.p_node) want)));
      t.want <- want;
      t.mailbox (Followers_chosen want)
    end

  (* Rebalance: lease counts lag by up to ttl/3, so shed only well above the
     fair share, and wait between rounds for the counts to settle. *)
  let rebalance_once t =
    let mine = Hashtbl.length t.runtimes in
    let n = 1 + List.length t.peers in
    let total = mine + List.fold_left (fun a l -> a + l.l_cells) 0 t.peers in
    let fair = (total + n - 1) / n in
    if rebalance () && t.peers <> [] && mine > fair + 1 && now_ms () - t.last_shed_ms > 3 * ttl_ms () then begin
      t.last_shed_ms <- now_ms ();
      (* Fill the least loaded peers up to the fair share, in turn. *)
      let peers = List.sort (fun a b -> compare a.l_cells b.l_cells) t.peers in
      let spare = ref (Hashtbl.fold (fun id _ acc -> id :: acc) t.runtimes []) and left = ref (mine - fair) in
      List.iter
        (fun l ->
          let k = min !left (fair - l.l_cells) in
          if k > 0 then begin
            let cells = List.filteri (fun i _ -> i < k) !spare in
            spare := List.filteri (fun i _ -> i >= k) !spare;
            left := !left - k;
            Log.info (fun m -> m "rebalance: %d cells here, fair share %d; shedding %d to %s" mine fair k l.l_node);
            t.mailbox (Shed { cells; target = { p_node = l.l_node; p_addr = l.l_addr } })
          end)
        peers
    end

  (* ---- the follower log on disk ----
     A follower writes every batch and every seal to its block device before
     answering, so an acked entry survives the follower restarting. Records
     are 'A' ^ CLA1 batch or 'S' ^ leader session, applied in order. When the
     active half is three quarters full, the live state is rewritten into the
     other half. *)

  let held_for t session =
    match Hashtbl.find_opt t.held session with
    | Some h -> h
    | None ->
        let h = { sealed = false; entries = Hashtbl.create 64 } in
        Hashtbl.replace t.held session h;
        h

  let apply_batch t (b : Wire.batch) =
    let h = held_for t b.leader in
    List.iter (fun (e : Wire.entry) -> Hashtbl.replace h.entries e.seq e) b.entries;
    (* The leader only acknowledges what is still untiered. *)
    Hashtbl.filter_map_inplace (fun seq e -> if seq < b.truncate_to then None else Some e) h.entries

  let apply_record t r =
    let body = String.sub r 1 (String.length r - 1) in
    match r.[0] with
    | 'A' -> ( match Wire.decode_batch body with Ok b -> apply_batch t b | Error e -> Log.warn (fun m -> m "follower log: bad batch: %s" e))
    | 'S' -> (held_for t body).sealed <- true
    | _ -> Log.warn (fun m -> m "follower log: unknown record")

  let live_records t =
    Hashtbl.fold
      (fun session h acc ->
        let entries = List.sort (fun (a : Wire.entry) b -> compare a.seq b.seq) (Hashtbl.fold (fun _ e l -> e :: l) h.entries []) in
        let a = if entries = [] then [] else [ "A" ^ Wire.encode_batch { leader = session; truncate_to = 0; entries } ] in
        let s = if h.sealed then [ "S" ^ session ] else [] in
        a @ s @ acc)
      t.held []

  (* Persist [record], then run [k] under the same lock, so memory changes
     in the order the disk records them. *)
  let persisted t record k =
    Lwt_mutex.with_lock t.disk_lock (fun () ->
        Lwt.catch
          (fun () ->
            (if D.used t.disk + String.length record + 32 > D.capacity t.disk * 3 / 4 then begin
               let live = live_records t in
               Log.info (fun m -> m "follower log: compacting %d bytes into %d records" (D.used t.disk) (List.length live));
               D.rewrite t.disk live
             end
             else Lwt.return_unit)
            >>= fun () -> D.append t.disk record >|= fun () -> Ok (k ()))
          (fun exn -> Lwt.return (Error (Printexc.to_string exn))))

  (* ---- takeover recovery gate (celld's recover_serial) ----
     CAS the dead log to recovering, seal its followers, upload their tails,
     CAS it to sealed. Returns the record's final etag. *)
  let seal_local t session =
    persisted t ("S" ^ session) (fun () ->
        let h = held_for t session in
        h.sealed <- true;
        Hashtbl.fold (fun _ e acc -> e :: acc) h.entries [])

  let recover t (l : lease) : (string, failure) result Lwt.t =
    let cas guard l = B.put t.bucket (Wire.lease_key l.l_node) ~guard:(B.Match guard) (Wire.lease_to_json l) in
    cas l.l_etag { l with l_log = Log_recovering } >>= function
    | Ok `Precondition_failed -> (
        (* Someone else moved it; done if it is sealed now. *)
        B.get t.bucket (Wire.lease_key l.l_node) >|= function
        | Ok (Some (body, etag)) -> (
            match Wire.lease_of_json body ~etag with
            | Ok r when r.l_session <> l.l_session || r.l_log = Log_sealed || r.l_log = Log_none -> Ok etag
            | _ -> Error (Definite "owner log is being recovered elsewhere"))
        | _ -> Error (Definite "cannot re-read owner lease"))
    | Error f -> Lwt.return (Error (fail_of f))
    | Ok (`Ok etag) ->
        Lwt_list.map_p
          (fun (p : peer) ->
            if p.p_node = t.me then seal_local t l.l_session >|= Result.to_option
            else
              peer_call t p.p_addr ~meth:"POST" ~path:"/peer/log/seal" (Wire.to_string (Wire.Obj [ ("session", Wire.Str l.l_session) ]))
              >|= function
              | Ok { status = 200; body; _ } -> ( match Wire.decode_batch body with Ok b -> Some b.entries | Error _ -> None)
              | _ -> None)
          l.l_followers
        >>= fun tails ->
        let reachable = List.filter_map Fun.id tails in
        if l.l_followers <> [] && reachable = [] then Lwt.return (Error (Definite "no follower of the dead log is reachable"))
        else
          let entries = List.sort_uniq (fun (a : Wire.entry) b -> compare a.seq b.seq) (List.concat reachable) in
          Log.info (fun m -> m "recovering log of %s/%s: %d entries from %d followers" l.l_node l.l_session (List.length entries) (List.length reachable));
          Lwt_list.for_all_s
            (fun (en : Wire.entry) ->
              match Celld_ltx.decode en.ltx with
              | Error _ -> Lwt.return true (* uncertified bytes are not acknowledged writes *)
              | Ok f ->
                  let key = Wire.ltx_prefix en.cell ^ Celld_ltx.key ~epoch:en.cell_epoch ~level:0 ~min_txid:f.header.min_txid ~max_txid:f.header.max_txid in
                  B.put t.bucket key en.ltx >|= function Ok (`Ok _) -> true | _ -> false)
            entries
          >>= fun uploaded ->
          if not uploaded then Lwt.return (Error (Definite "tail upload failed"))
          else
            cas etag { l with l_log = Log_sealed } >|= function
            | Ok (`Ok etag) -> Ok etag
            | Ok `Precondition_failed -> Error (Definite "lost the recovery claim")
            | Error f -> Error (fail_of f)

  (* Set once the alarm functions below exist: restore schedules a restored
     cell's alarm through it. *)
  let alarm_hook : (t -> cell_id -> int -> unit) ref = ref (fun _ _ _ -> ())
  let push_alarm t cell at = !alarm_hook t cell at

  let class_key c = "classes/" ^ c ^ ".js"

  (* ---- compaction and epoch GC ----
     Derived data, so none of it goes through the node log or the output
     gate: a lost snapshot or a failed delete only leaves more files. *)

  let gc t cell ~epoch ~upto =
    B.list t.bucket ~prefix:(Wire.ltx_prefix cell) >>= function
    | Error _ -> Lwt.return_unit
    | Ok keys ->
        let plen = String.length (Wire.ltx_prefix cell) in
        let rel = List.map (fun k -> String.sub k plen (String.length k - plen)) keys in
        let dead = Store.garbage rel ~epoch ~upto in
        Lwt_list.iter_s (fun k -> B.delete t.bucket (Wire.ltx_prefix cell ^ k) >|= ignore) dead >|= fun () ->
        if dead <> [] then Log.info (fun m -> m "gc %s e%d: deleted %d of %d files" cell epoch (List.length dead) (List.length keys))

  let compact t cell epoch txid snapshot =
    let key = Wire.ltx_prefix cell ^ Celld_ltx.key ~epoch ~level:1 ~min_txid:1L ~max_txid:(Int64.of_int txid) in
    B.put t.bucket key snapshot >>= function
    | Ok (`Ok _) -> gc t cell ~epoch ~upto:txid
    | _ -> Log.warn (fun m -> m "compaction of %s at txid %d failed" cell txid); Lwt.return_unit

  (* ---- restore ---- *)

  let rec restore ?(retried = false) t cell epoch =
    B.list t.bucket ~prefix:(Wire.ltx_prefix cell) >>= function
    | Error f -> Lwt.return (Error (string_of_failure (fail_of f)))
    | Ok keys -> (
        let plen = String.length (Wire.ltx_prefix cell) in
        let rel = List.map (fun k -> String.sub k plen (String.length k - plen)) keys in
        let chain = Store.plan rel in
        Lwt_list.map_s
          (fun k ->
            B.get t.bucket (Wire.ltx_prefix cell ^ k) >|= function
            | Ok (Some (body, _)) -> Celld_ltx.decode body
            | _ -> Error ("cannot read " ^ k))
          chain
        >>= fun files ->
        match List.partition Result.is_ok files with
        | ok, [] -> (
            match Store.restore ~sql:(Handlers.is_sql cell) (List.map Result.get_ok ok) with
            | Error e -> Lwt.return (Error e)
            | Ok store -> (
                (if Handlers.is_js cell then
                   B.get t.bucket (class_key (Handlers.class_of cell)) >|= function
                   | Ok (Some (script, _)) -> (
                       match store.sql with
                       | Some db -> Result.map Option.some (Celld_js.create ~script ~db)
                       | None -> Error "JS cell without a database")
                   | Ok None -> Error ("no script for class " ^ Handlers.class_of cell ^ "; PUT it to /.well-known/celld/classes/" ^ Handlers.class_of cell)
                   | Error f -> Error (string_of_failure (fail_of f))
                 else Lwt.return (Ok None))
                >>= function
                | Error e -> Lwt.return (Error e)
                | Ok js ->
                Hashtbl.replace t.runtimes cell { store; epoch; snap_txid = Int64.to_int store.txid; js };
                (* Each epoch opens with a snapshot of what it restored. *)
                if store.txid > 0L then capture t cell epoch store.txid (Store.snapshot store);
                Log.info (fun m -> m "restored %s at e%d (txid %Ld, %d files)" cell epoch store.txid (List.length ok));
                Option.iter (fun at -> push_alarm t cell at) (Handlers.alarm_of store);
                Lwt.return (Ok ())))
        (* A previous owner's GC can delete a file between our list and get. *)
        | _, Error _ :: _ when not retried -> restore ~retried:true t cell epoch
        | _, Error e :: _ -> Lwt.return (Error e)
        | _ -> Lwt.return (Error "unreachable"))

  (* ---- effect executor ---- *)

  let push t ev = t.mailbox ev

  let async_bucket t f = Lwt.async (fun () -> f () >|= push t)

  let exec t (fx : eff) =
    match fx with
    | Schedule_timer { timer; generation; at_ms } ->
        Lwt.async (fun () -> sleep_ms (at_ms - now_ms ()) >|= fun () -> push t (Timer_fired { timer; generation }))
    | Halt reason ->
        Log.err (fun m -> m "SELF-FENCE: %s" reason);
        exit 3
    | Check_drained { op } ->
        Lwt.async (fun () ->
            let rec wait n =
              if Queue.is_empty t.log then Lwt.return (Ok ())
              else if n = 0 then Lwt.return (Error (Definite "uploads pending"))
              else sleep_ms 100 >>= fun () -> wait (n - 1)
            in
            wait 50 >|= fun result -> push t (Drain_checked { op; result }))
    | Hand_off { cell; peer } ->
        Lwt.async (fun () ->
            peer_call t peer.p_addr ~meth:"POST" ~path:"/peer/activate" ~headers:[ ("x-cells-scope", cell) ] "" >|= function
            | Ok { status = 200; _ } -> Log.info (fun m -> m "handed %s to %s" cell peer.p_node)
            | _ -> Log.warn (fun m -> m "handoff of %s to %s not confirmed; it activates on next use" cell peer.p_node))
    | Exit_node reason ->
        Log.info (fun m -> m "%s: lease retired, exiting" reason);
        exit 0
    | Read_self_lease { op } ->
        async_bucket t (fun () ->
            B.get t.bucket (Wire.lease_key t.me) >|= fun r ->
            let result =
              match r with
              | Ok None -> Ok None
              | Ok (Some (body, etag)) -> (match Wire.lease_of_json body ~etag with Ok l -> Ok (Some l) | Error e -> Error (Definite e))
              | Error f -> Error (fail_of f)
            in
            Self_lease_read { op; result })
    | Cas_lease { op; guard; lease } ->
        async_bucket t (fun () ->
            B.put t.bucket (Wire.lease_key t.me) ~guard:(guard_of guard) (Wire.lease_to_json lease) >|= fun r ->
            Lease_cas_completed { op; result = cas_of r })
    | Recover_own_log { op; lease } -> async_bucket t (fun () -> recover t lease >|= fun result -> Own_log_recovered { op; result })
    | Activate_followers peers -> activate t peers
    | Read_owner { op; cell } ->
        async_bucket t (fun () ->
            B.get t.bucket (Wire.own_key cell) >|= fun r ->
            let result =
              match r with
              | Ok None -> Ok None
              | Ok (Some (body, etag)) -> (match Wire.owner_of_json body ~etag with Ok o -> Ok (Some o) | Error e -> Error (Definite e))
              | Error f -> Error (fail_of f)
            in
            Owner_read { op; cell; result })
    | Read_lease { op; cell; node } ->
        async_bucket t (fun () ->
            B.get t.bucket (Wire.lease_key node) >|= fun r ->
            let result =
              match r with
              | Ok None -> Ok None
              | Ok (Some (body, etag)) -> (match Wire.lease_of_json body ~etag with Ok l -> Ok (Some l) | Error e -> Error (Definite e))
              | Error f -> Error (fail_of f)
            in
            Lease_read { op; cell; result })
    | Recover_log { op; cell; lease } ->
        async_bucket t (fun () -> recover t lease >|= fun r -> Log_recovered { op; cell; result = Result.map ignore r })
    | Cas_owner { op; cell; guard; epoch } ->
        async_bucket t (fun () ->
            B.put t.bucket (Wire.own_key cell) ~guard:(guard_of guard) (Wire.owner_to_json ~node:(Some t.me) ~epoch) >|= fun r ->
            Owner_cas_completed { op; cell; result = cas_of r })
    | Restore { op; cell; epoch; takeover } ->
        Log.info (fun m -> m "activating %s at e%d%s" cell epoch (if takeover then " (takeover)" else ""));
        async_bucket t (fun () -> restore t cell epoch >|= fun result -> Restore_completed { op; cell; result })
    | Complete { req; result } -> (
        match Hashtbl.find_opt t.completions req with
        | Some u -> Hashtbl.remove t.completions req; Lwt.wakeup_later u result
        | None -> ())
    | Release { req; result } -> (
        match Hashtbl.find_opt t.releases req with
        | Some u -> Hashtbl.remove t.releases req; Lwt.wakeup_later u result
        | None -> ())
    | Await_durable { op; cell; epoch; position } ->
        t.durable_waiters <- { w_op = op; w_cell = cell; w_epoch = epoch; w_position = position } :: t.durable_waiters;
        check_durable t;
        (* One proof budget; the core treats a late proof as stale. *)
        Lwt.async (fun () ->
            sleep_ms 10_000 >|= fun () ->
            if List.exists (fun w -> w.w_op = op) t.durable_waiters then begin
              t.durable_waiters <- List.filter (fun w -> w.w_op <> op) t.durable_waiters;
              push t (Durable_reached { op; result = Error (Definite "proof deadline"); source = Bucket })
            end)
    | Verify_ownership { op; cell; epoch } ->
        async_bucket t (fun () ->
            B.get t.bucket (Wire.own_key cell) >|= fun r ->
            let result =
              match r with
              | Ok (Some (body, etag)) -> (
                  match Wire.owner_of_json body ~etag with
                  | Ok o -> Ok (o.o_node = Some t.me && o.o_epoch = epoch)
                  | Error e -> Error (Definite e))
              | Ok None -> Ok false
              | Error f -> Error (fail_of f)
            in
            Ownership_verified { op; result })
    | Ensure_durable { op; cell; epoch } ->
        Lwt.async (fun () ->
            let rec wait n =
              let top = match Hashtbl.find_opt t.runtimes cell with Some r -> Int64.to_int r.store.txid | None -> 0 in
              if covered t cell epoch (fun en -> en.uploaded) >= top then Lwt.return (Ok ())
              else if n = 0 then Lwt.return (Error (Definite "uploads pending"))
              else sleep_ms 100 >>= fun () -> wait (n - 1)
            in
            wait 100 >|= fun result -> push t (Durability_checked { op; cell; result }))
    | Stop_runtime { op; cell; epoch; reset } ->
        Log.info (fun m -> m "stopping %s at e%d%s" cell epoch (if reset then " (reset)" else ""));
        (match Hashtbl.find_opt t.runtimes cell with Some { js = Some js; _ } -> Celld_js.dispose js | _ -> ());
        Hashtbl.remove t.runtimes cell;
        Hashtbl.remove t.alarm_timers cell;
        (* Ensure_durable ran first, so this epoch's files are all in the
           bucket and older epochs are dead weight. *)
        if not reset then Lwt.async (fun () -> gc t cell ~epoch ~upto:0);
        push t (Runtime_stopped { op; cell })
    | Release_owner { op; cell; guard; epoch } ->
        async_bucket t (fun () ->
            B.put t.bucket (Wire.own_key cell) ~guard:(guard_of guard) (Wire.owner_to_json ~node:None ~epoch) >|= fun r ->
            Log.info (fun m -> m "released %s at e%d" cell epoch);
            Owner_released { op; cell; result = cas_of r })

  (* ---- the serial actor ---- *)

  let actor t stream =
    Lwt_stream.iter (fun ev -> List.iter (exec t) (Core.on_event t.core ~now_ms:(now_ms ()) ev)) stream

  (* ---- serving a cell request ---- *)

  let text status body = { Celld_http.status; headers = [ ("content-type", "text/plain") ]; body }

  let fresh_req t =
    t.next_req <- t.next_req + 1;
    t.next_req

  let resolve t cell =
    let req = fresh_req t in
    let p, u = Lwt.wait () in
    Hashtbl.replace t.completions req u;
    push t (Request { req; cell });
    p >|= fun r -> (req, r)

  (* ---- alarms ----
     The owner keeps a timer per resident cell with an alarm. The bucket
     index alarms/<cell> lets any node wake a cell that was evicted before
     its alarm came due. *)

  let alarm_index cell = "alarms/" ^ cell

  let rec fire t cell =
    serve_cell t ~forwarded:true cell ~meth:"ALARM" ~path:"/__alarm/fire" ~query:[] ~body:"" >|= fun (r : Celld_http.response) ->
    if r.status = 200 && r.body = "fired\n" then Log.info (fun m -> m "alarm fired for %s" cell);
    r

  and schedule_alarm t cell at =
    Hashtbl.replace t.alarm_timers cell at;
    Lwt.async (fun () ->
        sleep_ms (at - now_ms ()) >>= fun () ->
        (* Superseded, cleared or evicted: the index covers an evicted cell. *)
        if Hashtbl.find_opt t.alarm_timers cell = Some at then (Hashtbl.remove t.alarm_timers cell; fire t cell >|= ignore)
        else Lwt.return_unit)

  (* A commit changed the cell's alarm. *)
  and alarm_changed t cell alarm =
    (match alarm with Some at -> schedule_alarm t cell at | None -> Hashtbl.remove t.alarm_timers cell);
    Lwt.async (fun () ->
        let rec go n =
          (match alarm with
          | Some at -> B.put t.bucket (alarm_index cell) (string_of_int at) >|= Result.is_ok
          | None -> B.delete t.bucket (alarm_index cell) >|= Result.is_ok)
          >>= fun ok -> if ok || n = 0 then Lwt.return_unit else sleep_ms 500 >>= fun () -> go (n - 1)
        in
        go 5)

  (* Wake due alarms of cells no live node holds. A fire that finds nothing
     due removes the index entry it read, unless it changed meanwhile. *)
  and scan_alarms t =
    B.list t.bucket ~prefix:"alarms/" >>= function
    | Error _ -> Lwt.return_unit
    | Ok keys ->
        Lwt_list.iter_s
          (fun k ->
            let cell = String.sub k 7 (String.length k - 7) in
            if Hashtbl.mem t.runtimes cell || not (Handlers.valid_cell cell) then Lwt.return_unit
            else
              B.get t.bucket k >>= function
              | Ok (Some (body, etag)) -> (
                  match int_of_string_opt body with
                  | Some at when at <= now_ms () ->
                      fire t cell >>= fun (r : Celld_http.response) ->
                      if r.status = 200 && r.body = "not due\n" then B.delete t.bucket ~guard:(B.Match etag) k >|= ignore
                      else Lwt.return_unit
                  | _ -> Lwt.return_unit)
              | _ -> Lwt.return_unit)
          keys

  and run_local t req cell epoch ~meth ~path ~query ~body =
    let handler rt = match rt.js with Some js -> Some (Handlers.js js cell) | None -> Handlers.dispatch cell in
    match Hashtbl.find_opt t.runtimes cell, Option.bind (Hashtbl.find_opt t.runtimes cell) handler with
    | Some rt, Some handler when rt.epoch = epoch ->
        (* Handler and commit run without yielding: the cell is serial. *)
        let alarm_before = Handlers.alarm_of rt.store in
        let now = now_ms () in
        Celld_sqlite.set_now (Int64.of_int now);
        let reply = handler rt.store ~now_ms:now ~meth ~path ~query ~body in
        let position =
          if reply.wrote then begin
            let txid, ltx = Store.commit rt.store in
            capture t cell epoch txid ltx;
            let alarm = Handlers.alarm_of rt.store in
            if alarm <> alarm_before then alarm_changed t cell alarm;
            let txid = Int64.to_int txid in
            if compact_every () > 0 && txid - rt.snap_txid >= compact_every () then begin
              rt.snap_txid <- txid;
              let snapshot = Store.snapshot rt.store in
              Lwt.async (fun () -> compact t cell epoch txid snapshot)
            end;
            Some txid
          end
          else None
        in
        let p, u = Lwt.wait () in
        Hashtbl.replace t.releases req u;
        push t (Output_at { req; position });
        (* The output gate: nothing leaves until the core releases it. *)
        p >|= (function
        | Ok () ->
            {
              Celld_http.status = reply.status;
              headers = [ ("content-type", reply.content_type); ("x-celld-node", t.me); ("x-celld-epoch", string_of_int epoch) ];
              body = reply.body;
            }
        | Error e -> text 503 ("write not acknowledged: " ^ string_of_request_error e ^ "\n"))
    | _ ->
        let p, u = Lwt.wait () in
        Hashtbl.replace t.releases req u;
        push t (Output_at { req; position = None });
        p >|= fun _ -> text 503 "cell runtime is not resident\n"

  and serve_cell t ?(forwarded = false) ?(retried = false) cell ~meth ~path ~query ~body =
    resolve t cell >>= fun (req, route) ->
    match route with
    | Error e -> Lwt.return (text 503 (string_of_request_error e ^ "\n"))
    | Ok (Local epoch) -> run_local t req cell epoch ~meth ~path ~query ~body
    | Ok (Remote _) when forwarded -> Lwt.return (text 421 "not the owner\n")
    | Ok (Remote peer) -> (
        let target = path ^ (if query = [] then "" else "?" ^ String.concat "&" (List.map (fun (k, v) -> Celld_http.pct_encode k ^ "=" ^ Celld_http.pct_encode v) query)) in
        peer_call t peer.p_addr ~meth:"POST" ~path:"/peer/forward"
          ~headers:[ ("x-cells-scope", cell); ("x-cells-method", meth); ("x-cells-path", target) ]
          body
        >>= function
        | Ok { status = 421; _ } | Error (`Connect _) when not retried ->
            (* NotOwner / NeverConnected: re-resolve once. *)
            push t (Invalidate_remote cell);
            serve_cell t ~retried:true cell ~meth ~path ~query ~body
        | Ok r -> Lwt.return { r with headers = List.filter (fun (k, _) -> String.length k > 7 && String.sub (String.lowercase_ascii k) 0 7 = "x-celld") r.headers }
        | Error (`Connect e) -> Lwt.return (text 502 ("owner unreachable: " ^ e ^ "\n"))
        | Error (`Io _ | `Timeout) ->
            (* Ambiguous: the owner may have run it. Never re-send. *)
            Lwt.return (text 504 "owner did not answer; the request may have run\n"))

  let () = alarm_hook := schedule_alarm

  let cell_of_target target =
    let path, query = Celld_http.path_and_query target in
    let prefix = "/do/" in
    let pl = String.length prefix in
    if String.length path > pl && String.sub path 0 pl = prefix then
      let rest = String.sub path pl (String.length path - pl) in
      let cell, sub = match String.index_opt rest '/' with Some i -> (String.sub rest 0 i, String.sub rest i (String.length rest - i)) | None -> (rest, "/") in
      Some (Celld_http.pct_decode cell, sub, query)
    else None

  let state_json t =
    let cells = List.map (fun (id, phase, active, barriers) -> Wire.Obj [ ("cell", Str id); ("phase", Str phase); ("active", Int active); ("barriers", Int barriers) ]) (Core.cells t.core) in
    Wire.to_string
      (Wire.Obj
         [
           ("node", Str t.me);
           ("session", Str t.session);
           ("durability", Str (if t.fleet then "fleet" else "bucket"));
           ("followers", Arr (List.map (fun p -> Wire.Str p.p_node) t.active));
           ("log_backlog", Int (Queue.length t.log));
           ("cells", Arr cells);
         ])
    ^ "\n"

  let public t (r : Celld_http.request) =
    match cell_of_target r.target with
    | _ when fst (Celld_http.path_and_query r.target) = "/.well-known/celld/health" -> Lwt.return (text 200 "ok\n")
    | _ when r.meth = "POST" && fst (Celld_http.path_and_query r.target) = "/.well-known/celld/drain" ->
        let targets = List.map (fun l -> { p_node = l.l_node; p_addr = l.l_addr }) t.peers in
        Log.info (fun m -> m "draining to [%s]" (String.concat "," (List.map (fun p -> p.p_node) targets)));
        push t (Drain targets);
        Lwt.return (text 202 "draining\n")
    | _ when String.length (fst (Celld_http.path_and_query r.target)) > 27
             && String.sub (fst (Celld_http.path_and_query r.target)) 0 27 = "/.well-known/celld/classes/" -> (
        let path = fst (Celld_http.path_and_query r.target) in
        let cls = String.sub path 27 (String.length path - 27) in
        if not (Handlers.valid_class cls) || Handlers.dispatch (cls ^ ":x") <> None then Lwt.return (text 400 "bad class name\n")
        else
          match r.meth with
          | "PUT" -> (
              B.put t.bucket (class_key cls) r.body >|= function
              | Ok (`Ok _) ->
                  Log.info (fun m -> m "deployed class %s (%d bytes)" cls (String.length r.body));
                  text 200 ("deployed " ^ cls ^ "; cells pick it up when they next activate\n")
              | _ -> text 503 "could not store the script\n")
          | "GET" -> (B.get t.bucket (class_key cls) >|= function Ok (Some (s, _)) -> { Celld_http.status = 200; headers = [ ("content-type", "text/javascript") ]; body = s } | _ -> text 404 "no such class\n")
          | _ -> Lwt.return (text 405 "PUT or GET\n"))
    | _ when fst (Celld_http.path_and_query r.target) = "/state" -> Lwt.return { Celld_http.status = 200; headers = [ ("content-type", "application/json") ]; body = state_json t }
    | Some (cell, path, query) when Handlers.valid_cell cell && (Handlers.dispatch cell <> None || Handlers.is_js cell) ->
        serve_cell t cell ~meth:r.meth ~path ~query ~body:r.body
    | Some _ -> Lwt.return (text 404 "unknown cell class (Counter:<id>, Kv:<id> or Sql:<id>)\n")
    | None -> Lwt.return (text 404 "try /do/Counter:<id>/increment or /do/Kv:<id>/<key>\n")

  let internal t (r : Celld_http.request) =
    match verify t r with
    | Error e -> Lwt.return (text 401 (e ^ "\n"))
    | Ok source -> (
        match r.meth, fst (Celld_http.path_and_query r.target) with
        | "POST", "/peer/forward" -> (
            let h k = Celld_http.header k r.headers in
            match h "x-cells-scope", h "x-cells-method", h "x-cells-path" with
            | Some cell, Some meth, Some target when Handlers.valid_cell cell ->
                let path, query = Celld_http.path_and_query target in
                serve_cell t ~forwarded:true cell ~meth ~path ~query ~body:r.body
            | _ -> Lwt.return (text 400 "bad forward\n"))
        | "POST", "/peer/activate" -> (
            (* Resolve the cell as a request would, without running a handler. *)
            match Celld_http.header "x-cells-scope" r.headers with
            | Some cell when Handlers.valid_cell cell && (Handlers.dispatch cell <> None || Handlers.is_js cell) -> (
                (* A cached route may still name the node handing it over. *)
                push t (Invalidate_remote cell);
                resolve t cell >>= fun (req, route) ->
                match route with
                | Ok (Local _) ->
                    let p, u = Lwt.wait () in
                    Hashtbl.replace t.releases req u;
                    push t (Output_at { req; position = None });
                    p >|= fun _ -> text 200 "active\n"
                | Ok (Remote p) -> Lwt.return (text 409 ("owned by " ^ p.p_node ^ "\n"))
                | Error e -> Lwt.return (text 503 (string_of_request_error e ^ "\n")))
            | _ -> Lwt.return (text 400 "bad activate\n"))
        | "POST", "/peer/log/append" -> (
            match Wire.decode_batch r.body with
            | Error e -> Lwt.return (text 400 (e ^ "\n"))
            | Ok b -> (
                if (held_for t b.leader).sealed then Lwt.return (text 409 "sealed\n")
                else
                  (* Sealing takes the same lock, so this re-check is final. *)
                  persisted t ("A" ^ r.body) (fun () ->
                      if (held_for t b.leader).sealed then false else (apply_batch t b; true))
                  >|= function
                  | Ok true -> text 200 (Printf.sprintf "{\"ok\":true,\"end\":%d}\n" (List.fold_left (fun a (e : Wire.entry) -> max a e.seq) 0 b.entries))
                  | Ok false -> text 409 "sealed\n"
                  | Error e -> text 503 ("follower log write failed: " ^ e ^ "\n")))
        | "POST", "/peer/log/seal" -> (
            match Wire.of_string r.body with
            | Ok j ->
                let session = Wire.str_field "session" j in
                (seal_local t session >|= function
                 | Ok entries ->
                     Log.info (fun m -> m "sealed log %s for %s (%d entries)" session source (List.length entries));
                     { Celld_http.status = 200; headers = []; body = Wire.encode_batch { leader = session; truncate_to = 0; entries } }
                 | Error e -> text 503 ("follower log write failed: " ^ e ^ "\n"))
            | Error e -> Lwt.return (text 400 (e ^ "\n")))
        | _ -> Lwt.return (text 404 "unknown peer endpoint\n"))

  (* The fleet secret: created once, conditionally, by whichever node is first. *)
  let rec peer_secret bucket =
    B.get bucket Wire.peer_auth_key >>= function
    | Ok (Some (body, _)) -> ( match Wire.of_string body with Ok j -> Lwt.return (Wire.str_field "secret" j) | Error _ -> failwith "bad peer-auth.json")
    | Ok None -> (
        let secret = Wire.hex (Mirage_crypto_rng.generate 32) in
        B.put bucket ~guard:B.Absent Wire.peer_auth_key (Wire.to_string (Wire.Obj [ ("secret", Str secret) ])) >>= function
        | Ok (`Ok _) -> Lwt.return secret
        | _ -> peer_secret bucket)
    | Error _ -> sleep_ms 500 >>= fun () -> peer_secret bucket

  let start stack blk =
    let me = node_id () in
    D.open_ blk >>= fun (disk, records) ->
    let bip, bport = parse_addr (bucket_addr ()) in
    let bucket = B.create stack bip bport in
    let session = random_hex 8 in
    Celld_sqlite.seed (Mirage_crypto_rng.generate 32);
    let config = { default_config with ttl_ms = ttl_ms (); idle_ms = idle_ms () } in
    let core = Core.create ~config ~node:me ~session ~addr:(advertise ()) () in
    let stream, push = Lwt_stream.create () in
    let t =
      {
        stack; bucket; core; me; session;
        fleet = durability () = "fleet";
        secret = "";
        mailbox = (fun ev -> push (Some ev));
        runtimes = Hashtbl.create 16;
        log = Queue.create ();
        next_seq = 1;
        active = [];
        want = [];
        peers = [];
        last_shed_ms = 0;
        alarm_timers = Hashtbl.create 16;
        held = Hashtbl.create 4;
        disk;
        disk_lock = Lwt_mutex.create ();
        durable_waiters = [];
        completions = Hashtbl.create 64;
        releases = Hashtbl.create 64;
        next_req = 0;
        nonces = Hashtbl.create 64;
      }
    in
    List.iter (apply_record t) records;
    if records <> [] then
      Log.info (fun m ->
          m "follower log: replayed %d records, holding %d entries for %d leaders" (List.length records)
            (Hashtbl.fold (fun _ h n -> n + Hashtbl.length h.entries) t.held 0) (Hashtbl.length t.held));
    Log.info (fun m -> m "celld-ml node %s session %s, bucket %s, durability %s" me session (bucket_addr ()) (durability ()));
    peer_secret bucket >>= fun secret ->
    t.secret <- secret;
    H.serve stack ~port:internal_port (internal t);
    H.serve stack ~port:public_port (public t);
    Lwt.async (fun () -> actor t stream);
    t.mailbox Start_node;
    Lwt.async (fun () ->
        let rec loop () =
          discover t >>= fun () ->
          if t.fleet then recruit t;
          rebalance_once t;
          scan_alarms t >>= fun () ->
          sleep_ms 2_000 >>= loop
        in
        sleep_ms 500 >>= loop);
    S.listen stack
end
