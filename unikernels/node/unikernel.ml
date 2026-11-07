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
let bucket_addr = arg "bucket" "ip:port of the celld-ml bucket, or s3://ip:port/bucket for an S3-compatible store." Cmdliner.Arg.string "10.0.0.2:9000"
let s3_host = arg "s3-host" "Host header and TLS server name for S3 (default: the address)." Cmdliner.Arg.string ""
let s3_region = arg "s3-region" "S3 signing region." Cmdliner.Arg.string "us-east-1"
let s3_tls = arg "s3-tls" "Talk to S3 over TLS." Cmdliner.Arg.bool false
let s3_tls_fingerprint = arg "s3-tls-fingerprint" "Pin the S3 server certificate by SHA-256 fingerprint (hex) instead of trusting the Mozilla root store." Cmdliner.Arg.string ""
let ttl_ms = arg "ttl-ms" "Node lease lifetime." Cmdliner.Arg.int 10_000
let idle_ms = arg "idle-ms" "Evict a cell idle this long." Cmdliner.Arg.int 30_000
let durability = arg "durability" "fleet or bucket." Cmdliner.Arg.string "fleet"
let max_followers = arg "followers" "Followers to recruit in fleet mode." Cmdliner.Arg.int 2
let compact_every = arg "compact-every" "Upload a level-1 snapshot every this many commits, then delete what it covers." Cmdliner.Arg.int 64
let dns_server = arg "dns" "DNS server (IPv4) for outbound fetch from cells; without it only IP-literal URLs work." Cmdliner.Arg.string ""
let fetch_private = arg "fetch-private" "Let cells fetch private and loopback addresses (the bucket and peer ports stay refused)." Cmdliner.Arg.bool false
let max_memory_mb = arg "max-memory-mb" "Shed idle cells while cell memory (heap, SQLite pages, JS heaps) is above this; 0 disables." Cmdliner.Arg.int 0
let max_resident = arg "max-resident-cells" "Shed idle cells above this many resident; 0 disables." Cmdliner.Arg.int 0
let rebalance = arg "rebalance" "Shed cells to less loaded nodes." Cmdliner.Arg.bool true
let upload_delay_ms = arg "upload-delay-ms" "Delay before each LTX upload (to show the fleet path)." Cmdliner.Arg.int 0

let public_port = 8080
let internal_port = 8081

module Make (S : Tcpip.Stack.V4V6) (Blk : Mirage_block.S) (Sec : Mirage_block.S) = struct
  module H = Celld_http_mirage.Make (S)
  module B = Celld_bucketc.Make (S)
  module D = Celld_blocklog_mirage.Make (Blk)
  module Tls_flow = Tls_mirage.Make (S.TCP)

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

  type ws = { wconn : H.conn; wlock : Lwt_mutex.t }

  (* The event loop of one JS context (an interleaving cell, or a stateless
     worker): calls in flight, operations outstanding, socket effects not
     yet released, and whether a transaction holds the input gate. *)
  type loop = {
    lkey : string;
    mutable ljs : Celld_js.t;
    mutable next_call : int;
    waiters : (int, (string, string) result Lwt.u) Hashtbl.t;
    mutable pending_effects : string list;
    mutable outstanding : int;
    mutable gate : bool;
    deferred : (unit -> unit) Queue.t;
    mutable on_step : unit -> unit;  (** commit what the step wrote *)
    mutable rebuild : unit -> Celld_js.t option;
  }

  type worker = { mutable wetag : string; wloop : loop }

  type cache_entry = { c_status : int; c_headers : (string * string) list; c_body : string; c_expires : int; c_at : int }

  type runtime = {
    store : Store.t;
    epoch : int;
    mutable snap_txid : int;  (** last snapshot the bucket has *)
    mutable js : Celld_js.t option;  (** the class script's context, for JS classes *)
    mutable script : string;
    mutable script_etag : string;  (** the deployment this context runs *)
    mutable last_used : int;
    mutable last_alarm : int option;  (** the alarm as of the last commit *)
  }

  type t = {
    stack : S.t;
    bucket : B.t;
    core : Core.t;
    me : string;
    session : string;
    fleet : bool;
    mutable secret : string;
    mutable peer_client : Tls.Config.client option;  (** pins the fleet certificate *)
    mailbox : (event -> unit);
    runtimes : (cell_id, runtime) Hashtbl.t;
    log : entry Queue.t;  (** entries not yet uploaded, in seq order *)
    mutable next_seq : int;
    mutable active : peer list;  (** followers recorded in our lease and caught up *)
    mutable want : peer list;
    mutable peers : lease list;  (** live, non-draining leases of other nodes, from the last discovery *)
    mutable last_shed_ms : int;
    alarm_timers : (cell_id, int) Hashtbl.t;  (** the alarm each resident cell's timer waits for *)
    pumping : (cell_id, unit) Hashtbl.t;  (** cells whose outbox is being delivered *)
    tunnels : (string, H.conn list) Hashtbl.t;  (** idle peer tunnels, by peer address *)
    sockets : (cell_id, (string, ws) Hashtbl.t) Hashtbl.t;  (** WebSockets this node terminates, by cell *)
    cell_locks : (cell_id, Lwt_mutex.t) Hashtbl.t;  (** one handler at a time per cell, across a call's waits *)
    dns_cache : (string, Ipaddr.t * int) Hashtbl.t;  (** host -> address, expiry *)
    tls_clients : (string, Tls.Config.client) Hashtbl.t;  (** outbound HTTPS, per host *)
    bucket_ep : Ipaddr.t * int;
    class_versions : (string, string) Hashtbl.t;  (** newest known script etag per JS class *)
    loops : (string, loop) Hashtbl.t;  (** event loops of interleaving JS cells *)
    workers : (string, worker) Hashtbl.t;  (** stateless workers, by class *)
    cache : (string, cache_entry) Hashtbl.t;  (** the node-local Cache API store *)
    mutable cache_bytes : int;
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

  let tls_conn tls =
    {
      H.write =
        (fun s ->
          Tls_flow.write tls (Cstruct.of_string s) >|= function
          | Ok () -> Ok ()
          | Error e -> Error (Fmt.str "%a" Tls_flow.pp_write_error e));
      read =
        (fun () ->
          Tls_flow.read tls >|= function
          | Ok (`Data cs) -> `Data (Cstruct.to_string cs)
          | Ok `Eof -> `Eof
          | Error e -> `Error (Fmt.str "%a" Tls_flow.pp_error e));
      close = (fun () -> Tls_flow.close tls);
    }

  (* ---- the peer tunnel (celld's peer_tunnel) ----
     A TLS connection to a peer, pinned to the fleet certificate, upgraded
     once with a signed POST /peer/tunnel. After the 101 it carries plain
     keep-alive HTTP: idle tunnels are pooled per peer and reused for
     sequential calls, so a call costs no new handshake. A WebSocket takes a
     tunnel of its own and turns it into a byte relay. Every inner request
     is still HMAC-signed. *)

  let tls_open t addr =
    let ip, port = parse_addr addr in
    match t.peer_client with
    | None -> Lwt.return (Error "fleet TLS identity not loaded")
    | Some config -> (
        S.TCP.create_connection (S.tcp t.stack) (ip, port) >>= function
        | Error e -> Lwt.return (Error (Fmt.str "%a" S.TCP.pp_error e))
        | Ok flow -> (
            Tls_flow.client_of_flow config flow >|= function
            | Ok tls -> Ok (tls_conn tls)
            | Error e -> Error (Fmt.str "tls: %a" Tls_flow.pp_write_error e)))

  let signed t addr ?(headers = []) ~meth ~path body =
    { Celld_http.meth; target = path; headers = (("Host", addr) :: sign t ~meth ~path ~body) @ headers; body }

  let tunnel_open t addr =
    tls_open t addr >>= function
    | Error e -> Lwt.return (Error e)
    | Ok conn -> (
        H.exchange conn ~timeout_ns:5_000_000_000L
          (signed t addr ~headers:[ ("Upgrade", "celld-tunnel"); ("Connection", "Upgrade") ] ~meth:"POST" ~path:"/peer/tunnel" "")
        >>= function
        | Ok ({ status = 101; _ }, _) -> Lwt.return (Ok conn)
        | Ok (r, _) -> conn.close () >|= fun () -> Error (Printf.sprintf "tunnel refused: %d %s" r.status (String.trim r.body))
        | Error _ -> conn.close () >|= fun () -> Error "tunnel handshake failed")

  let pool_max = 4

  let take_tunnel t addr =
    match Hashtbl.find_opt t.tunnels addr with
    | Some (c :: rest) -> Hashtbl.replace t.tunnels addr rest; Some c
    | _ -> None

  let give_tunnel t addr conn =
    let idle = Option.value (Hashtbl.find_opt t.tunnels addr) ~default:[] in
    if List.length idle < pool_max then Hashtbl.replace t.tunnels addr (conn :: idle)
    else Lwt.async (fun () -> Lwt.catch conn.H.close (fun _ -> Lwt.return_unit))

  let rec peer_call ?(fresh = false) t addr ?(headers = []) ~meth ~path body =
    (match if fresh then None else take_tunnel t addr with
    | Some c -> Lwt.return (Ok (c, true))
    | None -> tunnel_open t addr >|= Result.map (fun c -> (c, false)))
    >>= function
    | Error e -> Lwt.return (Error (`Connect e))
    | Ok (conn, reused) -> (
        let drop () = Lwt.async (fun () -> Lwt.catch conn.H.close (fun _ -> Lwt.return_unit)) in
        H.exchange conn ~timeout_ns:5_000_000_000L (signed t addr ~headers ~meth ~path body) >>= function
        | Ok (resp, _) -> give_tunnel t addr conn; Lwt.return (Ok resp)
        (* A pooled tunnel the peer had already closed: the request never
           reached it, so it is safe to send again on a fresh one. *)
        | Error `Fresh_eof when reused -> drop (); peer_call ~fresh:true t addr ~headers ~meth ~path body
        | Error `Fresh_eof -> drop (); Lwt.return (Error (`Io "tunnel closed"))
        | Error (`Io e) -> drop (); Lwt.return (Error (`Io e))
        | Error `Timeout -> drop (); Lwt.return (Error `Timeout))

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
                | Ok l when l.l_node <> t.me && l.l_expires_ms > now_ms () && l.l_protocol = default_config.protocol && not l.l_draining -> Some l
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
                   | Ok (Some (script, etag)) -> (
                       match store.sql with
                       | Some db -> Result.map (fun js -> Some (js, (script, etag))) (Celld_js.create ~script ~db)
                       | None -> Error "JS cell without a database")
                   | Ok None -> Error ("no script for class " ^ Handlers.class_of cell ^ "; PUT it to /.well-known/celld/classes/" ^ Handlers.class_of cell)
                   | Error f -> Error (string_of_failure (fail_of f))
                 else Lwt.return (Ok None))
                >>= function
                | Error e -> Lwt.return (Error e)
                | Ok js ->
                Hashtbl.replace t.runtimes cell
                  {
                    store; epoch; snap_txid = Int64.to_int store.txid; js = Option.map fst js;
                    script = (match js with Some (_, (s, _)) -> s | None -> "");
                    script_etag = (match js with Some (_, (_, e)) -> e | None -> "");
                    last_used = now_ms ();
                    last_alarm = Handlers.alarm_of store;
                  };
                (* Each epoch opens with a snapshot of what it restored. *)
                if store.txid > 0L then capture t cell epoch store.txid (Store.snapshot store);
                Log.info (fun m -> m "restored %s at e%d (txid %Ld, %d files)" cell epoch store.txid (List.length ok));
                Option.iter (fun at -> push_alarm t cell at) (Handlers.alarm_of store);
                Lwt.return (Ok ())))
        (* A previous owner's GC can delete a file between our list and get. *)
        | _, Error _ :: _ when not retried -> restore ~retried:true t cell epoch
        | _, Error e :: _ -> Lwt.return (Error e)
        | _ -> Lwt.return (Error "unreachable"))

  let stop_runtime t op cell epoch reset =
    Log.info (fun m -> m "stopping %s at e%d%s" cell epoch (if reset then " (reset)" else ""));
    (match Hashtbl.find_opt t.runtimes cell with Some { js = Some js; _ } -> Celld_js.dispose js | _ -> ());
    (* The cell is leaving this node: its clients reconnect (1012, as
       celld and Cloudflare do on a move). *)
    (match Hashtbl.find_opt t.sockets cell with
    | Some socks ->
        Hashtbl.remove t.sockets cell;
        Hashtbl.iter
          (fun _ w ->
            Lwt.async (fun () ->
                Lwt_mutex.with_lock w.wlock (fun () -> w.wconn.write (Celld_ws.close_frame 1012 "cell moved") >>= fun _ -> w.wconn.close ())))
          socks
    | None -> ());
    Hashtbl.remove t.runtimes cell;
    Hashtbl.remove t.alarm_timers cell;
    (* Ensure_durable ran first, so this epoch's files are all in the
       bucket and older epochs are dead weight. *)
    if not reset then Lwt.async (fun () -> gc t cell ~epoch ~upto:0);
    t.mailbox (Runtime_stopped { op; cell })

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
        Lwt.async (fun () ->
            (* let in-flight calls and waitUntil work finish (bounded) *)
            let rec idle n =
              match Hashtbl.find_opt t.loops cell with
              | Some l when n > 0 && (Hashtbl.length l.waiters > 0 || l.outstanding > 0) -> sleep_ms 100 >>= fun () -> idle (n - 1)
              | _ -> Lwt.return_unit
            in
            idle 300 >|= fun () ->
            Hashtbl.remove t.loops cell;
            stop_runtime t op cell epoch reset)
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
    if r.status = 200 && r.body = "fired\n" then begin
      Log.info (fun m -> m "alarm fired for %s" cell);
      (* A queue's alarm means a batch may be due. *)
      if Handlers.class_of cell = "Queue" then Lwt.async (fun () -> pump_queue t cell 20)
    end;
    r

  (* Lease a batch from a local queue cell, run the consumer class's queue()
     on it in <Consumer>:__consumer (wherever that cell lives), and settle
     the outcome. A crash anywhere in between leaves the lease to expire, so
     the batch is redelivered. *)
  and pump_queue t cell n =
    if n = 0 then Lwt.return_unit
    else
      serve_cell t ~forwarded:true cell ~meth:"POST" ~path:"/__queue/lease" ~query:[] ~body:"" >>= fun (r : Celld_http.response) ->
      match if r.status = 200 then Wire.of_string r.body else Error "lease failed" with
      | Ok j -> (
          match Wire.field "messages" j with
          | Some (Wire.Arr (_ :: _ as msgs)) ->
              let consumer = Wire.str_field "consumer" j and batch = Wire.str_field "batch" j in
              serve_cell t (consumer ^ ":__consumer") ~meth:"QUEUE" ~path:"/__queue/deliver" ~query:[] ~body:r.body
              >>= fun (d : Celld_http.response) ->
              let ack, retry, failed =
                match if d.status = 200 then Wire.of_string d.body else Error "" with
                | Ok dj -> (Option.value (Wire.field "ack" dj) ~default:(Wire.Arr []), Option.value (Wire.field "retry" dj) ~default:(Wire.Arr []), false)
                | Error _ -> (Wire.Arr [], Wire.Arr [], true)
              in
              if failed then Log.warn (fun m -> m "%s: consumer %s failed a batch of %d: %s" cell consumer (List.length msgs) (String.trim d.body))
              else Log.info (fun m -> m "%s: delivered a batch of %d to %s" cell (List.length msgs) consumer);
              let settle = Wire.to_string (Wire.Obj [ ("batch", Wire.Str batch); ("ack", ack); ("retry", retry); ("failed", Wire.Bool failed) ]) in
              serve_cell t ~forwarded:true cell ~meth:"POST" ~path:"/__queue/settle" ~query:[] ~body:settle >>= fun _ -> pump_queue t cell (n - 1)
          | _ -> Lwt.return_unit)
      | Error _ -> Lwt.return_unit

  (* ---- WebSockets ----
     The node that owns a cell terminates its sockets; any other node only
     relays bytes over a tunnel. Each frame from a client is an event
     through the cell (webSocketMessage), serialized per socket, and what
     the cell sends back leaves after its writes are durable. *)

  and ws_write t cell id frame =
    match Option.bind (Hashtbl.find_opt t.sockets cell) (fun s -> Hashtbl.find_opt s id) with
    | Some w -> Lwt_mutex.with_lock w.wlock (fun () -> w.wconn.write frame >|= ignore)
    | None -> Lwt.return_unit

  and ws_forget t cell id =
    match Hashtbl.find_opt t.sockets cell with
    | Some s -> Hashtbl.remove s id; if Hashtbl.length s = 0 then Hashtbl.remove t.sockets cell
    | None -> ()

  and apply_effects t cell effects =
    match Wire.of_string effects with
    | Error _ -> Lwt.return_unit
    | Ok j ->
        let items k = match Wire.field k j with Some (Wire.Arr l) -> l | _ -> [] in
        Lwt_list.iter_s
          (fun m ->
            let id = Wire.str_field "id" m and data = Wire.str_field "data" m in
            let binary = Wire.field "binary" m = Some (Wire.Bool true) in
            ws_write t cell id (if binary then Celld_ws.encode Binary (Celld_ws.base64_decode data) else Celld_ws.encode Text data))
          (items "sends")
        >>= fun () ->
        Lwt_list.iter_s
          (fun m ->
            let id = Wire.str_field "id" m in
            let code = match Wire.int_field "code" m with 0 -> 1000 | c -> c in
            match Option.bind (Hashtbl.find_opt t.sockets cell) (fun s -> Hashtbl.find_opt s id) with
            | Some w ->
                ws_forget t cell id;
                Lwt_mutex.with_lock w.wlock (fun () -> w.wconn.write (Celld_ws.close_frame code (Wire.str_field "reason" m)) >>= fun _ -> w.wconn.close ())
            | None -> Lwt.return_unit)
          (items "closes")

  (* One client socket on the owner node. *)
  and ws_session t cell id conn initial =
    let socks = match Hashtbl.find_opt t.sockets cell with Some s -> s | None -> let s = Hashtbl.create 4 in Hashtbl.replace t.sockets cell s; s in
    let w = { wconn = conn; wlock = Lwt_mutex.create () } in
    Hashtbl.replace socks id w;
    Log.info (fun m -> m "%s: websocket %s open" cell id);
    let event path fields =
      serve_cell t ~forwarded:true cell ~meth:"WS" ~path ~query:[] ~body:(Wire.to_string (Wire.Obj (("id", Wire.Str id) :: fields)))
    in
    let finish code reason =
      let mine = match Option.bind (Hashtbl.find_opt t.sockets cell) (fun s -> Hashtbl.find_opt s id) with Some w' -> w' == w | None -> false in
      if not mine then Lwt.catch conn.close (fun _ -> Lwt.return_unit)
      else begin
        ws_forget t cell id;
        Log.info (fun m -> m "%s: websocket %s closed (%d)" cell id code);
        event "/__ws/close" [ ("code", Wire.Int code); ("reason", Wire.Str reason) ] >>= fun _ -> Lwt.catch conn.close (fun _ -> Lwt.return_unit)
      end
    in
    let frag = Buffer.create 256 and frag_binary = ref false in
    let rec loop buf =
      match Celld_ws.decode buf with
      | `Error e ->
          Lwt_mutex.with_lock w.wlock (fun () -> conn.write (Celld_ws.close_frame 1002 e)) >>= fun _ -> finish 1002 e
      | `Partial -> (
          conn.read () >>= function
          | `Data d -> loop (buf ^ d)
          | `Eof | `Error _ -> finish 1006 "")
      | `Frame (f, used) -> (
          let rest = String.sub buf used (String.length buf - used) in
          let message binary data =
            event "/__ws/message" [ ("data", Wire.Str (if binary then Celld_ws.base64 data else data)); ("binary", Wire.Bool binary) ] >>= fun (r : Celld_http.response) ->
            if r.status = 421 || r.status = 503 then
              Lwt_mutex.with_lock w.wlock (fun () -> conn.write (Celld_ws.close_frame 1012 "cell moved")) >>= fun _ -> finish 1012 "cell moved"
            else loop rest
          in
          match f.opcode with
          | (Text | Binary) when f.fin -> message (f.opcode = Binary) f.payload
          | Text | Binary ->
              Buffer.clear frag;
              Buffer.add_string frag f.payload;
              frag_binary := f.opcode = Binary;
              loop rest
          | Continuation ->
              Buffer.add_string frag f.payload;
              if Buffer.length frag > 1 lsl 20 then
                Lwt_mutex.with_lock w.wlock (fun () -> conn.write (Celld_ws.close_frame 1009 "message too big")) >>= fun _ -> finish 1009 ""
              else if f.fin then message !frag_binary (Buffer.contents frag)
              else loop rest
          | Ping -> Lwt_mutex.with_lock w.wlock (fun () -> conn.write (Celld_ws.encode Pong f.payload)) >>= fun _ -> loop rest
          | Pong | Other _ -> loop rest
          | Close ->
              let code, reason = Celld_ws.close_info f.payload in
              Lwt_mutex.with_lock w.wlock (fun () -> conn.write (Celld_ws.close_frame (if code = 1005 then 1000 else code) "")) >>= fun _ ->
              finish code reason)
    in
    Lwt.catch (fun () -> loop initial) (fun exn -> finish 1011 (Printexc.to_string exn))

  (* A client's upgrade request for [cell]. On the owner: run fetch, and on
     a 101 that accepted a socket, complete the handshake and keep the
     connection. Elsewhere: forward the upgrade over a dedicated tunnel and
     relay bytes both ways. *)
  and ws_upgrade t ?(forwarded = false) cell ~path ~query ~(headers : (string * string) list) =
    let key = Option.value (Celld_http.header "sec-websocket-key" headers) ~default:"" in
    resolve t cell >>= fun (req, route) ->
    match route with
    | Error e -> Lwt.return (H.Respond (text 503 (string_of_request_error e ^ "\n")))
    | Ok (Local epoch) -> (
        run_local t req cell epoch ~headers ~meth:"GET" ~path ~query ~body:"" () >|= fun (r : Celld_http.response) ->
        match r.status, Celld_http.header "x-celld-ws-accept" r.headers with
        | 101, Some id when key <> "" ->
            let hs =
              { Celld_http.status = 101; headers = [ ("Upgrade", "websocket"); ("Connection", "Upgrade"); ("Sec-WebSocket-Accept", Celld_ws.accept_key key) ]; body = "" }
            in
            H.Upgrade (hs, fun conn rest -> ws_session t cell id conn rest)
        | _ -> H.Respond r)
    | Ok (Remote _) when forwarded -> Lwt.return (H.Respond (text 421 "not the owner\n"))
    | Ok (Remote peer) -> (
        let target = path ^ if query = [] then "" else "?" ^ String.concat "&" (List.map (fun (k, v) -> Celld_http.pct_encode k ^ "=" ^ Celld_http.pct_encode v) query) in
        let pass = List.filter (fun (k, _) -> match String.lowercase_ascii k with "sec-websocket-key" | "sec-websocket-version" | "sec-websocket-protocol" | "origin" -> true | _ -> false) headers in
        tunnel_open t peer.p_addr >>= function
        | Error e -> Lwt.return (H.Respond (text 502 ("owner unreachable: " ^ e ^ "\n")))
        | Ok tun -> (
            H.exchange tun ~timeout_ns:10_000_000_000L
              (signed t peer.p_addr ~meth:"POST" ~path:"/peer/forward"
                 ~headers:([ ("x-cells-scope", cell); ("x-cells-method", "GET"); ("x-cells-path", target); ("x-cells-upgrade", "websocket") ] @ pass)
                 "")
            >>= function
            | Ok (({ status = 101; _ } as hs), tun_rest) ->
                Lwt.return (H.Upgrade (hs, fun client client_rest -> relay client client_rest tun tun_rest))
            | Ok (r, _) -> tun.close () >|= fun () -> H.Respond r
            | Error _ -> tun.close () >|= fun () -> H.Respond (text 502 "owner did not accept the upgrade\n")))

  and relay (a : H.conn) a_rest (b : H.conn) b_rest =
    let close_both () = Lwt.catch a.close (fun _ -> Lwt.return_unit) >>= fun () -> Lwt.catch b.close (fun _ -> Lwt.return_unit) in
    let rec pump (src : H.conn) (dst : H.conn) =
      src.read () >>= function
      | `Data d -> ( dst.write d >>= function Ok () -> pump src dst | Error _ -> close_both ())
      | `Eof | `Error _ -> close_both ()
    in
    (if b_rest <> "" then a.write b_rest >|= ignore else Lwt.return_unit) >>= fun () ->
    (if a_rest <> "" then b.write a_rest >|= ignore else Lwt.return_unit) >>= fun () ->
    Lwt.join [ pump a b; pump b a ]

  (* Deliver a cell's outbox to the queues it names, then acknowledge it in
     the cell. Runs after a durable write, and from the periodic loop. *)
  and pump_outbox t cell =
    match Hashtbl.find_opt t.runtimes cell with
    | Some rt when not (Hashtbl.mem t.pumping cell) ->
        let pending = Handlers.outbox_pending rt.store in
        if pending = [] then Lwt.return_unit
        else begin
          Hashtbl.replace t.pumping cell ();
          Lwt.finalize
            (fun () ->
              Lwt_list.iter_s
                (fun (q, payloads, ids) ->
                  let qcell = "Queue:" ^ q in
                  if not (Handlers.valid_cell qcell) then Lwt.return_unit
                  else
                    serve_cell t qcell ~meth:"POST" ~path:"/__queue/send" ~query:[] ~body:payloads >>= fun (r : Celld_http.response) ->
                    if r.status = 200 then serve_cell t ~forwarded:true cell ~meth:"OUTBOX" ~path:"/__outbox/ack" ~query:[] ~body:ids >|= ignore
                    else (Log.warn (fun m -> m "%s: outbox to %s not delivered (%d); retrying later" cell qcell r.status); Lwt.return_unit))
                pending)
            (fun () -> Hashtbl.remove t.pumping cell; Lwt.return_unit)
        end
    | _ -> Lwt.return_unit

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

  and cell_lock t cell =
    match Hashtbl.find_opt t.cell_locks cell with
    | Some l -> l
    | None -> let l = Lwt_mutex.create () in Hashtbl.replace t.cell_locks cell l; l

  and run_local t req cell epoch ?(headers = []) ~meth ~path ~query ~body () =
    match Hashtbl.find_opt t.runtimes cell with
    | Some rt when rt.epoch = epoch && rt.js <> None && not (Handlers.is_strict_js_cell cell) ->
        run_loop_cell t req cell epoch rt ~headers ~meth ~path ~query ~body
    | Some rt when rt.epoch = epoch && (rt.js <> None || Handlers.dispatch cell <> None) ->
        (* The cell is serial: one handler at a time, including across a JS
           call's waits for I/O. *)
        Lwt_mutex.with_lock (cell_lock t cell) (fun () ->
            redeploy t cell rt >>= fun () ->
            rt.last_used <- now_ms ();
            let alarm_before = Handlers.alarm_of rt.store in
            let context () =
              let now = now_ms () in
              Celld_sqlite.set_now (Int64.of_int now);
              Celld_js.sockets := (match Hashtbl.find_opt t.sockets cell with Some s -> Hashtbl.fold (fun id _ acc -> id :: acc) s [] | None -> []);
              now
            in
            let first =
              let now = context () in
              Celld_js.headers := headers;
              let st =
                match rt.js with
                | Some js -> (try Handlers.js js cell rt.store ~now_ms:now ~meth ~path ~query ~body with Failure e -> Handlers.Done (Handlers.err 503 (e ^ "\n")))
                | None -> Handlers.Done ((Option.get (Handlers.dispatch cell)) rt.store ~now_ms:now ~meth ~path ~query ~body)
              in
              Celld_js.headers := [];
              st
            in
            let rec drive = function
              | Handlers.Done r -> Lwt.return r
              | Handlers.Await (ops, k) ->
                  perform_ops t cell ops >>= fun results ->
                  if not (match Hashtbl.find_opt t.runtimes cell with Some r -> r == rt | None -> false) then
                    Lwt.return (Handlers.err 503 "cell stopped while its request waited\n")
                  else begin
                    ignore (context ());
                    match k results with
                    | st -> drive st
                    | exception Failure e -> Lwt.return (Handlers.err 503 (e ^ "\n"))
                  end
            in
            drive first >|= fun reply ->
            Celld_js.sockets := [];
            (* A call that left promise jobs behind poisons its context. *)
            (match rt.js, rt.store.sql with
            | Some js, Some db when Celld_js.poisoned js -> (
                Celld_js.dispose js;
                match Celld_js.create ~script:rt.script ~db with
                | Ok fresh -> rt.js <- Some fresh; Log.warn (fun m -> m "%s: rebuilt a poisoned JS context" cell)
                | Error e -> rt.js <- None; Log.err (fun m -> m "%s: cannot rebuild JS context: %s" cell e))
            | _ -> ());
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
            (reply, position))
        >>= fun ((reply : Handlers.reply), position) ->
        let p, u = Lwt.wait () in
        Hashtbl.replace t.releases req u;
        push t (Output_at { req; position });
        (* The output gate: nothing leaves until the core releases it. *)
        p >|= (function
        | Ok () ->
            (* the write is durable: its outbox may go out, and its socket
               sends may leave *)
            if reply.wrote && rt.store.sql <> None then Lwt.async (fun () -> pump_outbox t cell);
            if reply.effects <> "" then Lwt.async (fun () -> apply_effects t cell reply.effects);
            let accept =
              match Wire.of_string reply.effects with
              | Ok j -> ( match Wire.field "accept" j with Some (Wire.Arr (Wire.Str id :: _)) -> [ ("x-celld-ws-accept", id) ] | _ -> [])
              | Error _ -> []
            in
            {
              Celld_http.status = reply.status;
              headers = [ ("content-type", reply.content_type); ("x-celld-node", t.me); ("x-celld-epoch", string_of_int epoch) ] @ accept;
              body = reply.body;
            }
        | Error e -> text 503 ("write not acknowledged: " ^ string_of_request_error e ^ "\n"))
    | _ ->
        let p, u = Lwt.wait () in
        Hashtbl.replace t.releases req u;
        push t (Output_at { req; position = None });
        p >|= fun _ -> text 503 "cell runtime is not resident\n"

  (* ---- operations a JS call waits for ----
     ops: JSON array of {id, kind, ...}; results: [{id, value} | {id, error}].
     Every operation has a deadline, so a call cannot hang its cell. *)
  (* ---- rolling deploys ----
     A resident JS cell runs the script version it was activated with. When
     a newer version of its class is known, the cell switches before its
     next event: calls are serial, so none is in flight, and its storage and
     WebSockets stay where they are. *)
  and redeploy t cell rt =
    match rt.js, rt.store.sql, Hashtbl.find_opt t.class_versions (Handlers.class_of cell) with
    | Some old, Some db, Some v when v <> rt.script_etag -> (
        B.get t.bucket (class_key (Handlers.class_of cell)) >|= function
        | Ok (Some (script, etag)) -> (
            match Celld_js.create ~script ~db with
            | Ok fresh ->
                Celld_js.dispose old;
                rt.js <- Some fresh;
                rt.script <- script;
                rt.script_etag <- etag;
                Hashtbl.replace t.class_versions (Handlers.class_of cell) etag;
                Log.info (fun m -> m "%s: now runs deployment %s" cell etag)
            | Error e ->
                (* keep the old code; the next event tries again *)
                Log.err (fun m -> m "%s: deployment %s does not load: %s" cell etag e))
        | _ -> ())
    | _ -> Lwt.return_unit

  (* ---- the event loop ----
     Calls start on the context and run until they await; the node polls
     for finished calls and queued operations, performs those concurrently,
     and resumes the context with each result as it arrives. Every step
     commits what it wrote, so a call that finishes is released only once
     everything written before that point is durable. *)
  and loop_wake (l : loop) call result =
    match Hashtbl.find_opt l.waiters call with
    | Some u -> Hashtbl.remove l.waiters call; Lwt.wakeup_later u result
    | None -> ()

  and loop_fail (l : loop) e =
    let ws = Hashtbl.fold (fun k u acc -> (k, u) :: acc) l.waiters [] in
    Hashtbl.reset l.waiters;
    List.iter (fun (_, u) -> Lwt.wakeup_later u (Error e)) ws

  and loop_pump t (l : loop) =
    Celld_sqlite.set_now (Int64.of_int (now_ms ()));
    (match Celld_js.invoke l.ljs "__celld_poll" "" with
    | Error e -> loop_fail l e
    | Ok p -> (
        match Wire.of_string p with
        | Error _ -> loop_fail l "unreadable poll"
        | Ok j ->
            (match Wire.field "effects" j with Some (Wire.Str e) when e <> "" -> l.pending_effects <- l.pending_effects @ [ e ] | _ -> ());
            l.gate <- Wire.field "gate" j = Some (Wire.Bool true);
            (* commit before anyone sees a result *)
            l.on_step ();
            (match Wire.field "done" j with
            | Some (Wire.Arr ds) ->
                List.iter
                  (fun d ->
                    loop_wake l (Wire.int_field "call" d)
                      (match Wire.field "out" d with Some (Wire.Str o) -> Ok o | _ -> Error (Wire.str_field "error" d)))
                  ds
            | _ -> ());
            (match Wire.field "ops" j with Some (Wire.Arr ops) -> List.iter (fun op -> loop_op t l op) ops | _ -> ());
            if not l.gate then
              while not (Queue.is_empty l.deferred) do
                (Queue.pop l.deferred) ()
              done));
    if Celld_js.poisoned l.ljs then begin
      loop_fail l "the context was poisoned (promise jobs left behind) and is rebuilt";
      match l.rebuild () with Some js -> l.ljs <- js | None -> ()
    end

  and loop_op t (l : loop) op =
    let js = l.ljs in
    l.outstanding <- l.outstanding + 1;
    let id = Wire.int_field "_op" op and kind = Wire.str_field "kind" op in
    let deadline = if kind = "sleep" then Wire.int_field "ms" op + 5_000 else 30_000 in
    Lwt.async (fun () ->
        Lwt.pick
          [
            (Lwt.catch (fun () -> perform_op t l.lkey kind op) (fun exn -> Lwt.return (Error (Printexc.to_string exn))));
            (sleep_ms deadline >|= fun () -> Error (kind ^ " timed out"));
          ]
        >|= fun res ->
        l.outstanding <- l.outstanding - 1;
        (* a rebuilt context never sees results meant for the old one *)
        if l.ljs == js then begin
          let r = match res with Ok v -> Wire.Obj [ ("_op", Wire.Int id); ("value", v) ] | Error e -> Wire.Obj [ ("_op", Wire.Int id); ("error", Wire.Str e) ] in
          Celld_sqlite.set_now (Int64.of_int (now_ms ()));
          ignore (Celld_js.invoke js "__celld_resume" (Wire.to_string (Wire.Arr [ r ])));
          loop_pump t l
        end)

  and loop_call t (l : loop) ~name ~input =
    let p, u = Lwt.wait () in
    let start () =
      l.next_call <- l.next_call + 1;
      let id = l.next_call in
      Hashtbl.replace l.waiters id u;
      Celld_sqlite.set_now (Int64.of_int (now_ms ()));
      (match Celld_js.invoke l.ljs "__celld_start" (Wire.to_string (Wire.Obj [ ("call", Wire.Int id); ("name", Wire.Str name); ("input", Wire.Str input) ])) with
      | Error e -> loop_wake l id (Error e)
      | Ok _ -> ());
      loop_pump t l
    in
    (* an open transaction holds the input gate *)
    if l.gate then Queue.push start l.deferred else start ();
    Lwt.pick [ p; (sleep_ms 60_000 >|= fun () -> Error "the call did not settle within 60 s") ]

  and new_loop key js = {
    lkey = key; ljs = js; next_call = 0; waiters = Hashtbl.create 8; pending_effects = []; outstanding = 0; gate = false;
    deferred = Queue.create (); on_step = (fun () -> ()); rebuild = (fun () -> None);
  }

  (* Commit what an interleaving cell wrote, outside any open transaction. *)
  and commit_step t cell rt =
    match rt.store.sql with
    | Some db when Celld_sqlite.autocommit db && Store.changed rt.store ->
        let txid, ltx = Store.commit rt.store in
        capture t cell rt.epoch txid ltx;
        let alarm = Handlers.alarm_of rt.store in
        if alarm <> rt.last_alarm then (rt.last_alarm <- alarm; alarm_changed t cell alarm);
        let txid = Int64.to_int txid in
        if compact_every () > 0 && txid - rt.snap_txid >= compact_every () then begin
          rt.snap_txid <- txid;
          let snapshot = Store.snapshot rt.store in
          Lwt.async (fun () -> compact t cell rt.epoch txid snapshot)
        end;
        if Hashtbl.mem t.runtimes cell then Lwt.async (fun () -> pump_outbox t cell)
    | _ -> ()

  and cell_loop t cell rt =
    match Hashtbl.find_opt t.loops cell, rt.js with
    | Some l, Some js when l.ljs == js -> l
    | _, js ->
        let l = new_loop cell (Option.get js) in
        l.on_step <- (fun () -> commit_step t cell rt);
        l.rebuild <-
          (fun () ->
            match rt.store.sql with
            | Some db -> (
                (match rt.js with Some old -> Celld_js.dispose old | None -> ());
                match Celld_js.create ~script:rt.script ~db with
                | Ok fresh -> rt.js <- Some fresh; Log.warn (fun m -> m "%s: rebuilt its JS context" cell); Some fresh
                | Error e -> rt.js <- None; Log.err (fun m -> m "%s: cannot rebuild JS context: %s" cell e); None)
            | None -> None);
        Hashtbl.replace t.loops cell l;
        l

  (* A request to an interleaving JS cell. *)
  and run_loop_cell t req cell epoch rt ~headers ~meth ~path ~query ~body =
    let now = now_ms () in
    Celld_sqlite.set_now (Int64.of_int now);
    let finish_with (reply : Handlers.reply) effects =
      commit_step t cell rt;
      let position = if rt.store.txid > 0L then Some (Int64.to_int rt.store.txid) else None in
      let p, u = Lwt.wait () in
      Hashtbl.replace t.releases req u;
      push t (Output_at { req; position });
      p >|= function
      | Ok () ->
          List.iter (fun e -> Lwt.async (fun () -> apply_effects t cell e)) effects;
          let accept =
            List.find_map
              (fun e -> match Wire.of_string e with Ok j -> (match Wire.field "accept" j with Some (Wire.Arr (Wire.Str id :: _)) -> Some ("x-celld-ws-accept", id) | _ -> None) | Error _ -> None)
              effects
          in
          {
            Celld_http.status = reply.status;
            headers = [ ("content-type", reply.content_type); ("x-celld-node", t.me); ("x-celld-epoch", string_of_int epoch) ] @ Option.to_list accept;
            body = reply.body;
          }
      | Error e -> text 503 ("write not acknowledged: " ^ string_of_request_error e ^ "\n")
    in
    match Handlers.js_loop_route rt.store ~now_ms:now ~meth ~path ~query ~body with
    | Handlers.Reply_now r -> finish_with r []
    | Handlers.Call { name; body = call_body; finish } ->
        let l = cell_loop t cell rt in
        (* a new deployment loads only between calls *)
        (if Hashtbl.length l.waiters = 0 && l.outstanding = 0 then
           Lwt_mutex.with_lock (cell_lock t cell) (fun () -> redeploy t cell rt >|= fun () -> match rt.js with Some js -> l.ljs <- js | None -> ())
         else Lwt.return_unit)
        >>= fun () ->
        rt.last_used <- now_ms ();
        let sockets = match Hashtbl.find_opt t.sockets cell with Some s -> Hashtbl.fold (fun id _ acc -> id :: acc) s [] | None -> [] in
        let input = Celld_js.input_json ~cell ~meth ~path ~query ~headers ~sockets ~body:call_body () in
        loop_call t l ~name ~input >>= fun res ->
        let reply = finish (Result.bind res Celld_js.parse_output) in
        let effects = l.pending_effects in
        l.pending_effects <- [];
        finish_with reply effects

  (* ---- stateless workers ----
     A class's fetch, run on whichever node received the request, with no
     cell, no storage and no ownership: /w/<Class>/... and ctx.service().
     One context per class per node, on an event loop. *)
  and worker_for t cls =
    let fresh () =
      B.get t.bucket (class_key cls) >|= function
      | Ok (Some (script, etag)) -> (
          match Celld_js.create ~script ~db:(Celld_sqlite.open_image "") with
          | Ok js ->
              let w = { wetag = etag; wloop = new_loop ("worker:" ^ cls) js } in
              Hashtbl.replace t.workers cls w;
              Ok w
          | Error e -> Error e)
      | Ok None -> Error ("no class " ^ cls)
      | Error f -> Error (string_of_failure (fail_of f))
    in
    match Hashtbl.find_opt t.workers cls with
    | Some w ->
        let stale = match Hashtbl.find_opt t.class_versions cls with Some v -> v <> w.wetag | None -> false in
        if stale && Hashtbl.length w.wloop.waiters = 0 && w.wloop.outstanding = 0 then (Celld_js.dispose w.wloop.ljs; fresh ()) else Lwt.return (Ok w)
    | None -> fresh ()

  and run_worker t cls ~meth ~path ~query ~headers ~body =
    if not (Handlers.valid_class cls) || Handlers.dispatch (cls ^ ":x") <> None then Lwt.return (text 400 "not a JS class\n")
    else
      worker_for t cls >>= function
      | Error e -> Lwt.return (text 503 (e ^ "\n"))
      | Ok w -> (
          let input = Celld_js.input_json ~stateless:true ~cell:("worker:" ^ cls) ~meth ~path ~query ~headers ~sockets:[] ~body () in
          loop_call t w.wloop ~name:"fetch" ~input >|= fun res ->
          w.wloop.pending_effects <- [];
          match Result.bind res Celld_js.parse_output with
          | Ok r -> { Celld_http.status = r.status; headers = ("x-celld-node", t.me) :: List.filter (fun (k, _) -> k <> "content-length" && k <> "connection" && k <> "transfer-encoding") r.headers; body = r.body }
          | Error e -> text 500 ("uncaught exception: " ^ e ^ "\n"))

  (* ---- the Cache API: node-local, bounded, honoring max-age ---- *)
  and cache_op t op =
    let key = Wire.str_field "name" op ^ "\n" ^ Wire.str_field "url" op in
    let now = now_ms () in
    match Wire.str_field "op" op with
    | "match" -> (
        match Hashtbl.find_opt t.cache key with
        | Some e when e.c_expires > now ->
            Ok (Wire.Obj [ ("status", Wire.Int e.c_status); ("headers", Wire.Obj (List.map (fun (k, v) -> (k, Wire.Str v)) e.c_headers)); ("body", Wire.Str (Celld_js.base64 e.c_body)) ])
        | Some e -> Hashtbl.remove t.cache key; t.cache_bytes <- t.cache_bytes - String.length e.c_body; Ok Wire.Null
        | None -> Ok Wire.Null)
    | "put" ->
        let body = Celld_js.unbase64 (Wire.str_field "body" op) in
        if String.length body > 8 lsl 20 then Error "a cached response is at most 8 MiB"
        else begin
          (match Hashtbl.find_opt t.cache key with Some e -> t.cache_bytes <- t.cache_bytes - String.length e.c_body | None -> ());
          let headers = match Wire.field "headers" op with Some (Wire.Obj l) -> List.filter_map (fun (k, v) -> match v with Wire.Str v -> Some (k, v) | _ -> None) l | _ -> [] in
          Hashtbl.replace t.cache key { c_status = Wire.int_field "status" op; c_headers = headers; c_body = body; c_expires = now + (Wire.int_field "ttl" op * 1000); c_at = now };
          t.cache_bytes <- t.cache_bytes + String.length body;
          (* evict the oldest entries past 32 MiB *)
          while t.cache_bytes > 32 lsl 20 && Hashtbl.length t.cache > 0 do
            let oldest = Hashtbl.fold (fun k e acc -> match acc with Some (_, a) when a.c_at <= e.c_at -> acc | _ -> Some (k, e)) t.cache None in
            match oldest with Some (k, e) -> Hashtbl.remove t.cache k; t.cache_bytes <- t.cache_bytes - String.length e.c_body | None -> ()
          done;
          Ok Wire.Null
        end
    | "delete" -> (
        match Hashtbl.find_opt t.cache key with
        | Some e -> Hashtbl.remove t.cache key; t.cache_bytes <- t.cache_bytes - String.length e.c_body; Ok (Wire.Bool true)
        | None -> Ok (Wire.Bool false))
    | _ -> Error "bad cache operation"

  and perform_ops t cell ops =
    match Wire.of_string ops with
    | Ok (Wire.Arr items) ->
        Lwt_list.map_p
          (fun op ->
            let id = Wire.int_field "_op" op and kind = Wire.str_field "kind" op in
            let deadline = if kind = "sleep" then Wire.int_field "ms" op + 5_000 else 30_000 in
            Lwt.pick
              [
                (Lwt.catch (fun () -> perform_op t cell kind op) (fun exn -> Lwt.return (Error (Printexc.to_string exn))));
                (sleep_ms deadline >|= fun () -> Error (kind ^ " timed out"));
              ]
            >|= function
            | Ok v -> Wire.Obj [ ("_op", Wire.Int id); ("value", v) ]
            | Error e -> Wire.Obj [ ("_op", Wire.Int id); ("error", Wire.Str e) ])
          items
        >|= fun results -> Wire.to_string (Wire.Arr results)
    | Ok _ | Error _ ->
        (* Every waiting promise gets an answer, or the call would hang. *)
        Log.err (fun m -> m "%s: unreadable operations from a JS call" cell);
        Lwt.return "[{\"_op\":0,\"error\":\"unreadable operations\"}]"

  and response_json (r : Celld_http.response) =
    Wire.Obj
      [
        ("status", Wire.Int r.status);
        ("headers", Wire.Obj (List.map (fun (k, v) -> (String.lowercase_ascii k, Wire.Str v)) r.headers));
        ("body", Wire.Str (Celld_js.base64 r.body));
      ]

  and perform_op t cell kind op =
    let str = Wire.str_field and std_method m = List.mem m [ "GET"; "HEAD"; "POST"; "PUT"; "PATCH"; "DELETE"; "OPTIONS" ] in
    match kind with
    | "sleep" -> sleep_ms (min 30_000 (max 0 (Wire.int_field "ms" op))) >|= fun () -> Ok Wire.Null
    | "fetch" -> outbound_fetch t op
    | "cell" -> (
        let target = str "cell" op and meth = String.uppercase_ascii (str "method" op) and target_path = str "path" op in
        if not (Handlers.valid_cell target) || not (String.contains target ':') then Lwt.return (Error ("bad cell id " ^ target))
        else if target = cell then Lwt.return (Error "a cell cannot call itself")
        else if not (std_method meth) then Lwt.return (Error ("method not allowed: " ^ meth))
        else if String.length target_path = 0 || target_path.[0] <> '/' || (String.length target_path > 2 && String.sub target_path 0 3 = "/__") then
          Lwt.return (Error "bad path")
        else
          let path, query = Celld_http.path_and_query target_path in
          serve_cell t target ~meth ~path ~query ~body:(Celld_js.unbase64 (str "body" op)) >|= fun r -> Ok (response_json r))
    | "d1" -> (
        let db = str "db" op in
        let target = "D1:" ^ db in
        if not (Handlers.valid_cell target) || String.contains db ':' then Lwt.return (Error ("bad database name " ^ db))
        else
          let body = Wire.to_string (Wire.Obj [ ("stmts", Option.value (Wire.field "stmts" op) ~default:(Wire.Arr [])); ("batch", Option.value (Wire.field "batch" op) ~default:(Wire.Bool false)) ]) in
          serve_cell t target ~meth:"POST" ~path:"/__d1" ~query:[] ~body >|= fun (r : Celld_http.response) ->
          if r.status <> 200 then Error (String.trim r.body) else Wire.of_string r.body)
    | "r2" -> r2_op t op
    | "cache" -> Lwt.return (cache_op t op)
    | "service" ->
        let target_path = str "path" op in
        let path, query = Celld_http.path_and_query (if target_path = "" then "/" else target_path) in
        let headers = match Wire.field "headers" op with Some (Wire.Obj l) -> List.filter_map (fun (k, v) -> match v with Wire.Str v -> Some (k, v) | _ -> None) l | _ -> [] in
        if not (std_method (String.uppercase_ascii (str "method" op))) then Lwt.return (Error "method not allowed")
        else
          run_worker t (str "class" op) ~meth:(String.uppercase_ascii (str "method" op)) ~path ~query ~headers ~body:(Celld_js.unbase64 (str "body" op)) >|= fun r ->
          Ok (response_json r)
    | "enqueue" -> (
        let q = str "queue" op in
        if not (Handlers.valid_cell ("Queue:" ^ q)) || String.contains q ':' then Lwt.return (Error "bad queue name")
        else
          let messages = Option.value (Wire.field "messages" op) ~default:(Wire.Arr []) in
          serve_cell t ("Queue:" ^ q) ~meth:"POST" ~path:"/__queue/send" ~query:[] ~body:(Wire.to_string messages) >|= fun (r : Celld_http.response) ->
          if r.status = 200 then Ok Wire.Null else Error ("queue refused the messages: " ^ String.trim r.body))
    | "workflow" -> (
        let cls = str "class" op and id = str "id" op and action = str "action" op in
        let target = cls ^ ":wf-" ^ id in
        if not (Handlers.valid_class cls) || Handlers.dispatch (cls ^ ":x") <> None || not (Handlers.valid_cell target) || String.contains id ':' then
          Lwt.return (Error "bad workflow class or id")
        else
          let path, body =
            match action with
            | "create" -> ("/__wf/create", Wire.to_string (Wire.Obj [ ("params", Option.value (Wire.field "params" op) ~default:Wire.Null) ]))
            | _ -> ("/__wf/status", "")
          in
          serve_cell t target ~meth:"WORKFLOW" ~path ~query:[] ~body >|= fun (r : Celld_http.response) ->
          if r.status <> 200 then Error (String.trim r.body) else Wire.of_string r.body)
    | k -> Lwt.return (Error ("unknown operation " ^ k))

  (* ---- R2 on the fleet's bucket: r2/<bucket>/<key>, metadata beside it ---- *)
  and r2_op t op =
    let str = Wire.str_field in
    let bucket = str "bucket" op and key = str "key" op in
    let valid_bucket = String.length bucket >= 3 && String.length bucket <= 63 && String.for_all (function 'a' .. 'z' | '0' .. '9' | '-' -> true | _ -> false) bucket in
    let valid_key = String.length key > 0 && String.length key <= 1024 && not (String.contains key '\000') in
    let obj_key = "r2/" ^ bucket ^ "/" ^ key and meta_key = "r2meta/" ^ bucket ^ "/" ^ key in
    if not valid_bucket then Lwt.return (Error "bad bucket name (3-63 of a-z, 0-9, -)")
    else
      match str "op" op with
      | ("get" | "head") as which -> (
          B.get t.bucket obj_key >>= function
          | Ok None -> Lwt.return (Ok Wire.Null)
          | Error f -> Lwt.return (Error (string_of_failure (fail_of f)))
          | Ok (Some (body, etag)) ->
              B.get t.bucket meta_key >|= fun meta ->
              let meta = match meta with Ok (Some (m, _)) -> (match Wire.of_string m with Ok j -> j | Error _ -> Wire.Obj []) | _ -> Wire.Obj [] in
              Ok
                (Wire.Obj
                   ([ ("size", Wire.Int (String.length body)); ("etag", Wire.Str etag);
                      ("uploaded", Option.value (Wire.field "uploaded" meta) ~default:(Wire.Int 0));
                      ("httpMetadata", Option.value (Wire.field "httpMetadata" meta) ~default:(Wire.Obj []));
                      ("customMetadata", Option.value (Wire.field "customMetadata" meta) ~default:(Wire.Obj [])) ]
                   @ if which = "get" then [ ("body", Wire.Str (Celld_js.base64 body)) ] else [])))
      | "put" when valid_key -> (
          let body = Celld_js.unbase64 (str "body" op) in
          B.put t.bucket obj_key body >>= function
          | Ok (`Ok etag) ->
              let uploaded = now_ms () in
              let meta =
                Wire.Obj
                  [ ("uploaded", Wire.Int uploaded);
                    ("httpMetadata", Option.value (Wire.field "httpMetadata" op) ~default:(Wire.Obj []));
                    ("customMetadata", Option.value (Wire.field "customMetadata" op) ~default:(Wire.Obj [])) ]
              in
              B.put t.bucket meta_key (Wire.to_string meta) >|= fun _ ->
              Ok (Wire.Obj [ ("size", Wire.Int (String.length body)); ("etag", Wire.Str etag); ("uploaded", Wire.Int uploaded) ])
          | Ok `Precondition_failed -> Lwt.return (Error "conflict")
          | Error f -> Lwt.return (Error (string_of_failure (fail_of f))))
      | "delete" when valid_key -> (B.delete t.bucket obj_key >>= fun _ -> B.delete t.bucket meta_key >|= fun _ -> Ok Wire.Null)
      | "list" -> (
          let prefix = "r2/" ^ bucket ^ "/" in
          B.list t.bucket ~prefix:(prefix ^ str "prefix" op) >|= function
          | Error f -> Error (string_of_failure (fail_of f))
          | Ok keys ->
              let cursor = str "cursor" op and limit = max 1 (min 1000 (Wire.int_field "limit" op)) in
              let pl = String.length prefix in
              let names = List.sort compare (List.map (fun k -> String.sub k pl (String.length k - pl)) keys) in
              let after = List.filter (fun k -> cursor = "" || compare k cursor > 0) names in
              let page = List.filteri (fun i _ -> i < limit) after in
              let truncated = List.length after > limit in
              Ok
                (Wire.Obj
                   [ ("keys", Wire.Arr (List.map (fun k -> Wire.Str k) page)); ("truncated", Wire.Bool truncated);
                     ("cursor", Wire.Str (if truncated then List.nth page (List.length page - 1) else "")) ]))
      | _ -> Lwt.return (Error "bad R2 operation or key")

  (* ---- outbound fetch ---- *)
  and dns_resolve t host =
    match Hashtbl.find_opt t.dns_cache host with
    | Some (ip, until) when until > now_ms () -> Lwt.return (Ok ip)
    | _ -> (
        match Ipaddr.V4.of_string (dns_server ()) with
        | Error _ -> Lwt.return (Error ("cannot resolve " ^ host ^ ": no DNS server (--dns)"))
        | Ok server ->
            let qid = Char.code (Mirage_crypto_rng.generate 1).[0] lsl 8 lor Char.code (Mirage_crypto_rng.generate 1).[0] in
            let port = 20_000 + (Char.code (Mirage_crypto_rng.generate 1).[0] * 150) in
            let q = Buffer.create 64 in
            Buffer.add_uint16_be q qid;
            Buffer.add_string q "\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00";
            List.iter (fun l -> Buffer.add_char q (Char.chr (String.length l)); Buffer.add_string q l) (String.split_on_char '.' host);
            Buffer.add_string q "\x00\x00\x01\x00\x01";
            let answer, wake = Lwt.wait () in
            S.UDP.listen (S.udp t.stack) ~port (fun ~src:_ ~dst:_ ~src_port:_ buf ->
                (if Lwt.is_sleeping answer then Lwt.wakeup_later wake (Cstruct.to_string buf));
                Lwt.return_unit);
            let parse r =
              (* header, question, then answers; names may be compressed *)
              let n = String.length r in
              let rec skip_name i = if i >= n then n else let l = Char.code r.[i] in if l = 0 then i + 1 else if l land 0xc0 = 0xc0 then i + 2 else skip_name (i + 1 + l) in
              if n < 12 || String.get_uint16_be r 0 <> qid then Error "bad DNS response"
              else if Char.code r.[3] land 0x0f <> 0 then Error ("cannot resolve " ^ host)
              else
                let an = String.get_uint16_be r 6 in
                let i = skip_name 12 + 4 in
                let rec answers k i =
                  if k = 0 || i + 10 > n then Error ("no address for " ^ host)
                  else
                    let i = skip_name i in
                    let typ = String.get_uint16_be r i and ttl = Int32.to_int (String.get_int32_be r (i + 4)) and len = String.get_uint16_be r (i + 8) in
                    if typ = 1 && len = 4 && i + 14 <= n then
                      Ok (Ipaddr.V4 (Ipaddr.V4.of_octets_exn (String.sub r (i + 10) 4)), max 1 ttl)
                    else answers (k - 1) (i + 10 + len)
                in
                answers an i
            in
            S.UDP.write ~src_port:port ~dst:(Ipaddr.V4 server) ~dst_port:53 (S.udp t.stack) (Cstruct.of_string (Buffer.contents q)) >>= fun _ ->
            Lwt.pick [ (answer >|= fun r -> Some r); (sleep_ms 3_000 >|= fun () -> None) ] >|= fun r ->
            S.UDP.unlisten (S.udp t.stack) ~port;
            match r with
            | None -> Error ("DNS timeout for " ^ host)
            | Some r -> (
                match parse r with
                | Ok (ip, ttl) -> Hashtbl.replace t.dns_cache host (ip, now_ms () + (min ttl 300 * 1000)); Ok ip
                | Error e -> Error e))

  (* Cells may not reach the bucket or the peer port (both trust the fleet),
     and by default nothing private or local. *)
  and fetch_allowed t ip port =
    let bip, bport = t.bucket_ep in
    let peer_hosts = List.filter_map (fun l -> Option.map fst (try Some (parse_addr l.l_addr) with Invalid_argument _ -> None)) t.peers in
    let own = fst (parse_addr (advertise ())) in
    if Ipaddr.compare ip bip = 0 && port = bport then Error "the bucket is not reachable from cells"
    else if port = internal_port && List.exists (fun h -> Ipaddr.compare h ip = 0) (own :: peer_hosts) then Error "peer ports are not reachable from cells"
    else if (not (fetch_private ())) && (match ip with
              | Ipaddr.V4 v4 ->
                  List.exists (fun (p, _) -> Ipaddr.V4.Prefix.mem v4 (Ipaddr.V4.Prefix.of_string_exn p))
                    [ ("10.0.0.0/8", ()); ("172.16.0.0/12", ()); ("192.168.0.0/16", ()); ("127.0.0.0/8", ()); ("169.254.0.0/16", ()); ("0.0.0.0/8", ()); ("100.64.0.0/10", ()) ]
              | Ipaddr.V6 v6 -> Ipaddr.V6.is_private v6 || Ipaddr.V6.compare v6 Ipaddr.V6.localhost = 0)
    then Error "private addresses are not reachable from cells (--fetch-private)"
    else Ok ()

  and outbound_fetch t op =
    let url = Wire.str_field "url" op in
    let scheme, rest =
      if String.length url > 8 && String.sub url 0 8 = "https://" then ("https", String.sub url 8 (String.length url - 8))
      else if String.length url > 7 && String.sub url 0 7 = "http://" then ("http", String.sub url 7 (String.length url - 7))
      else ("", url)
    in
    if scheme = "" then Lwt.return (Error ("only http and https URLs: " ^ url))
    else
      let authority, target = match String.index_opt rest '/' with Some i -> (String.sub rest 0 i, String.sub rest i (String.length rest - i)) | None -> (rest, "/") in
      let authority = match String.index_opt authority '@' with Some i -> String.sub authority (i + 1) (String.length authority - i - 1) | None -> authority in
      let target = match String.index_opt target '#' with Some i -> String.sub target 0 i | None -> target in
      let host, port =
        match String.rindex_opt authority ':' with
        | Some i when not (String.contains authority ']') -> (String.sub authority 0 i, int_of_string_opt (String.sub authority (i + 1) (String.length authority - i - 1)))
        | _ -> (authority, None)
      in
      let port = Option.value port ~default:(if scheme = "https" then 443 else 80) in
      (match Ipaddr.of_string host with Ok ip -> Lwt.return (Ok ip) | Error _ -> dns_resolve t host) >>= function
      | Error e -> Lwt.return (Error e)
      | Ok ip -> (
          match fetch_allowed t ip port with
          | Error e -> Lwt.return (Error e)
          | Ok () -> (
              let tls =
                if scheme = "http" then Ok None
                else
                  match Hashtbl.find_opt t.tls_clients host with
                  | Some c -> Ok (Some c)
                  | None -> (match B.tls_config ~host () with Ok c -> Hashtbl.replace t.tls_clients host c; Ok (Some c) | Error e -> Error e)
              in
              match tls with
              | Error e -> Lwt.return (Error ("TLS: " ^ e))
              | Ok tls ->
                  let connect () =
                    match tls with
                    | None -> H.tcp_connect t.stack ip port
                    | Some config -> (
                        S.TCP.create_connection (S.tcp t.stack) (ip, port) >>= function
                        | Error e -> Lwt.return (Error (Fmt.str "%a" S.TCP.pp_error e))
                        | Ok flow -> (
                            let peer = match Domain_name.of_string host with Ok d -> Result.to_option (Domain_name.host d) | Error _ -> None in
                            Tls_flow.client_of_flow config ?host:peer flow >|= function
                            | Ok c -> Ok (tls_conn c)
                            | Error e -> Error (Fmt.str "tls: %a" Tls_flow.pp_write_error e)))
                  in
                  let headers =
                    match Wire.field "headers" op with
                    | Some (Wire.Obj l) ->
                        List.filter_map
                          (fun (k, v) -> match String.lowercase_ascii k, v with ("host" | "connection" | "content-length" | "transfer-encoding"), _ -> None | k, Wire.Str v -> Some (k, v) | _ -> None)
                          l
                    | _ -> []
                  in
                  let default_host = if (scheme = "http" && port = 80) || (scheme = "https" && port = 443) then host else host ^ ":" ^ string_of_int port in
                  H.request_over ~connect ~default_host ~timeout_ns:30_000_000_000L
                    { Celld_http.meth = String.uppercase_ascii (Wire.str_field "method" op); target; headers; body = Celld_js.unbase64 (Wire.str_field "body" op) }
                  >|= function
                  | Ok r when String.length r.body > 16 lsl 20 -> Error "response larger than 16 MiB"
                  | Ok r -> Ok (response_json r)
                  | Error (`Connect e) -> Error ("connect: " ^ e)
                  | Error (`Io e) -> Error e
                  | Error `Timeout -> Error "timeout"))

  and serve_cell t ?(forwarded = false) ?(retried = false) cell ~meth ~path ~query ~body =
    resolve t cell >>= fun (req, route) ->
    match route with
    | Error e -> Lwt.return (text 503 (string_of_request_error e ^ "\n"))
    | Ok (Local epoch) -> run_local t req cell epoch ~meth ~path ~query ~body ()
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
        | Ok r ->
            Lwt.return
              { r with headers = List.filter (fun (k, _) -> let k = String.lowercase_ascii k in k = "content-type" || (String.length k > 7 && String.sub k 0 7 = "x-celld")) r.headers }
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
    let cells =
      List.map
        (fun (id, phase, active, barriers) ->
          let code = match Hashtbl.find_opt t.runtimes id with Some rt when rt.script_etag <> "" -> [ ("deployment", Wire.Str rt.script_etag) ] | _ -> [] in
          Wire.Obj ([ ("cell", Wire.Str id); ("phase", Wire.Str phase); ("active", Wire.Int active); ("barriers", Wire.Int barriers) ] @ code))
        (Core.cells t.core)
    in
    Wire.to_string
      (Wire.Obj
         [
           ("node", Str t.me);
           ("session", Str t.session);
           ("durability", Str (if t.fleet then "fleet" else "bucket"));
           ("followers", Arr (List.map (fun p -> Wire.Str p.p_node) t.active));
           ("log_backlog", Int (Queue.length t.log));
           ("resident", Int (Hashtbl.length t.runtimes));
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
    | _ when String.length (fst (Celld_http.path_and_query r.target)) > 26
             && String.sub (fst (Celld_http.path_and_query r.target)) 0 26 = "/.well-known/celld/queues/" -> (
        let path = fst (Celld_http.path_and_query r.target) in
        let name = String.sub path 26 (String.length path - 26) in
        let qcell = "Queue:" ^ name in
        if not (Handlers.valid_cell qcell) || String.contains name ':' then Lwt.return (text 400 "bad queue name\n")
        else
          match r.meth with
          | "PUT" -> (
              (* Validate the consumer settings against celld's bounds. *)
              match Wire.of_string r.body with
              | Error e -> Lwt.return (text 400 ("bad JSON: " ^ e ^ "\n"))
              | Ok j ->
                  let int k = Wire.int_field k j and has k = Wire.field k j <> None in
                  let bad =
                    if not (Handlers.valid_class (Wire.str_field "consumer" j)) then Some "consumer must name a JS class"
                    else if has "max_batch_size" && (int "max_batch_size" < 1 || int "max_batch_size" > 100) then Some "max_batch_size is 1-100"
                    else if has "max_batch_timeout_ms" && (int "max_batch_timeout_ms" < 0 || int "max_batch_timeout_ms" > 60_000) then Some "max_batch_timeout_ms is 0-60000"
                    else if has "max_retries" && (int "max_retries" < 0 || int "max_retries" > 100) then Some "max_retries is 0-100"
                    else if has "retry_delay_ms" && (int "retry_delay_ms" < 0 || int "retry_delay_ms" > 86_400_000) then Some "retry_delay_ms is 0-86400000"
                    else if has "dead_letter_queue" && not (Handlers.valid_cell ("Queue:" ^ Wire.str_field "dead_letter_queue" j)) then Some "bad dead_letter_queue"
                    else None
                  in
                  match bad with
                  | Some e -> Lwt.return (text 400 (e ^ "\n"))
                  | None -> serve_cell t qcell ~meth:"POST" ~path:"/__queue/config" ~query:[] ~body:r.body)
          | "GET" -> serve_cell t qcell ~meth:"GET" ~path:"/" ~query:[] ~body:""
          | _ -> Lwt.return (text 405 "PUT or GET\n"))
    | _ when String.length (fst (Celld_http.path_and_query r.target)) > 33
             && String.ends_with ~suffix:"/crons" (fst (Celld_http.path_and_query r.target))
             && String.sub (fst (Celld_http.path_and_query r.target)) 0 27 = "/.well-known/celld/classes/" -> (
        let path = fst (Celld_http.path_and_query r.target) in
        let cls = String.sub path 27 (String.length path - 27 - 6) in
        if not (Handlers.valid_class cls) || Handlers.dispatch (cls ^ ":x") <> None then Lwt.return (text 400 "bad class name\n")
        else if r.meth <> "PUT" then Lwt.return (text 405 "PUT a newline-separated list of cron expressions\n")
        else
          let exprs = List.filter (fun e -> e <> "") (List.map String.trim (String.split_on_char '\n' r.body)) in
          match List.find_map (fun e -> match Celld_cron.parse e with Error m -> Some (e ^ ": " ^ m) | Ok _ -> None) exprs with
          | Some e -> Lwt.return (text 400 ("bad cron expression " ^ e ^ "\n"))
          | None -> serve_cell t (cls ^ ":__cron") ~meth:"CRON" ~path:"/__cron/arm" ~query:[] ~body:(String.concat "\n" exprs))
    | _ when String.length (fst (Celld_http.path_and_query r.target)) > 27
             && String.sub (fst (Celld_http.path_and_query r.target)) 0 27 = "/.well-known/celld/classes/" -> (
        let path = fst (Celld_http.path_and_query r.target) in
        let cls = String.sub path 27 (String.length path - 27) in
        if not (Handlers.valid_class cls) || Handlers.dispatch (cls ^ ":x") <> None then Lwt.return (text 400 "bad class name\n")
        else
          match r.meth with
          | "PUT" -> (
              B.put t.bucket (class_key cls) r.body >|= function
              | Ok (`Ok etag) ->
                  Log.info (fun m -> m "deployed class %s (%d bytes) as %s" cls (String.length r.body) etag);
                  Hashtbl.replace t.class_versions cls etag;
                  (* tell the fleet; the periodic check covers a missed peer *)
                  let note = Wire.to_string (Wire.Obj [ ("class", Wire.Str cls); ("etag", Wire.Str etag) ]) in
                  List.iter (fun l -> Lwt.async (fun () -> peer_call t l.l_addr ~meth:"POST" ~path:"/peer/deploy" note >|= ignore)) t.peers;
                  text 200 ("deployed " ^ cls ^ " as " ^ etag ^ "; resident cells switch at their next event\n")
              | _ -> text 503 "could not store the script\n")
          | "GET" -> (B.get t.bucket (class_key cls) >|= function Ok (Some (s, _)) -> { Celld_http.status = 200; headers = [ ("content-type", "text/javascript") ]; body = s } | _ -> text 404 "no such class\n")
          | _ -> Lwt.return (text 405 "PUT or GET\n"))
    | _ when String.length (fst (Celld_http.path_and_query r.target)) > 3 && String.sub (fst (Celld_http.path_and_query r.target)) 0 3 = "/w/" -> (
        (* a stateless worker: /w/<Class>/<path> *)
        let path, query = Celld_http.path_and_query r.target in
        let rest = String.sub path 3 (String.length path - 3) in
        let cls, sub = match String.index_opt rest '/' with Some i -> (String.sub rest 0 i, String.sub rest i (String.length rest - i)) | None -> (rest, "/") in
        if not (List.mem r.meth [ "GET"; "HEAD"; "POST"; "PUT"; "PATCH"; "DELETE"; "OPTIONS" ]) then Lwt.return (text 405 "method not allowed\n")
        else run_worker t cls ~meth:r.meth ~path:sub ~query ~headers:r.headers ~body:r.body)
    | _ when fst (Celld_http.path_and_query r.target) = "/state" -> Lwt.return { Celld_http.status = 200; headers = [ ("content-type", "application/json") ]; body = state_json t }
    (* Runtime paths and methods are for the node itself, never clients. *)
    | Some (_, path, _)
      when (String.length path > 2 && String.sub path 0 3 = "/__" && path <> "/__alarm")
           || not (List.mem r.meth [ "GET"; "HEAD"; "POST"; "PUT"; "PATCH"; "DELETE"; "OPTIONS" ]) ->
        Lwt.return (text 403 "reserved for the runtime\n")
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
        | "POST", "/peer/deploy" -> (
            match Wire.of_string r.body with
            | Ok j when Handlers.valid_class (Wire.str_field "class" j) ->
                Hashtbl.replace t.class_versions (Wire.str_field "class" j) (Wire.str_field "etag" j);
                Lwt.return (text 200 "ok\n")
            | _ -> Lwt.return (text 400 "bad deploy note\n"))
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
  (* Every few rounds, learn the current script version of each class with
     resident cells (a deploy note from a peer can be lost). *)
  let deploy_round = ref 0

  let check_deployments t =
    incr deploy_round;
    if !deploy_round mod 5 = 0 then begin
      let classes = Hashtbl.fold (fun cell rt acc -> if rt.js <> None && not (List.mem (Handlers.class_of cell) acc) then Handlers.class_of cell :: acc else acc) t.runtimes [] in
      List.iter
        (fun cls -> Lwt.async (fun () -> B.get t.bucket (class_key cls) >|= function Ok (Some (_, etag)) -> Hashtbl.replace t.class_versions cls etag | _ -> ()))
        classes
    end

  (* Memory the cells hold: the OCaml heap, SQLite page images, JS heaps. *)
  let cell_memory t =
    let heap = (Gc.quick_stat ()).heap_words * (Sys.word_size / 8) in
    Hashtbl.fold
      (fun _ rt acc -> acc + (Array.length rt.store.pages * Store.page_size) + match rt.js with Some js -> Celld_js.memory js | None -> 0)
      t.runtimes heap

  (* Shed the least recently used idle cells (no sockets, no handler
     running) while over the memory budget or the residency cap. *)
  let shed_if_needed t =
    let resident = Hashtbl.length t.runtimes in
    let mem = cell_memory t in
    let over_cap = if max_resident () > 0 then resident - max_resident () else 0 in
    let over_mem = max_memory_mb () > 0 && mem > max_memory_mb () * 1_048_576 in
    if over_cap > 0 || over_mem then begin
      let idle =
        Hashtbl.fold
          (fun cell rt acc ->
            let busy = match Hashtbl.find_opt t.cell_locks cell with Some l -> Lwt_mutex.is_locked l | None -> false in
            if busy || Hashtbl.mem t.sockets cell then acc else (rt.last_used, cell) :: acc)
          t.runtimes []
        |> List.sort compare |> List.map snd
      in
      let k = max over_cap (if over_mem then max 1 (resident / 10) else 0) in
      let victims = List.filteri (fun i _ -> i < k) idle in
      if victims <> [] then begin
        Log.warn (fun m -> m "shedding %d idle cells (%d resident, %d MiB of cell memory)" (List.length victims) resident (mem / 1_048_576));
        t.mailbox (Evict victims)
      end
    end

  let is_ws_upgrade (r : Celld_http.request) =
    r.meth = "GET"
    && (match Celld_http.header "upgrade" r.headers with Some v -> String.lowercase_ascii (String.trim v) = "websocket" | None -> false)

  (* The peer port: a signed /peer/tunnel upgrade turns the connection into
     a tunnel serving the same endpoints; a forwarded WebSocket upgrade
     hands the tunnel to the socket. Everything else is [internal]. *)
  let rec internal_ext t (r : Celld_http.request) =
    match r.meth, fst (Celld_http.path_and_query r.target) with
    | "POST", "/peer/tunnel" -> (
        match verify t r with
        | Error e -> Lwt.return (H.Respond (text 401 (e ^ "\n")))
        | Ok _ ->
            let hs = { Celld_http.status = 101; headers = [ ("Upgrade", "celld-tunnel"); ("Connection", "Upgrade") ]; body = "" } in
            Lwt.return (H.Upgrade (hs, fun conn rest -> H.handle_conn_ext ~initial:rest (internal_ext t) conn)))
    | "POST", "/peer/forward" when Celld_http.header "x-cells-upgrade" r.headers = Some "websocket" -> (
        match verify t r with
        | Error e -> Lwt.return (H.Respond (text 401 (e ^ "\n")))
        | Ok _ -> (
            match Celld_http.header "x-cells-scope" r.headers, Celld_http.header "x-cells-path" r.headers with
            | Some cell, Some target when Handlers.valid_cell cell && Handlers.is_js cell ->
                let path, query = Celld_http.path_and_query target in
                ws_upgrade t ~forwarded:true cell ~path ~query ~headers:(("upgrade", "websocket") :: r.headers)
            | _ -> Lwt.return (H.Respond (text 400 "bad websocket forward\n"))))
    | _ -> internal t r >|= fun x -> H.Respond x

  (* The public port: WebSocket upgrades for JS cells, else [public]. *)
  let public_ext t (r : Celld_http.request) =
    match cell_of_target r.target with
    | Some (cell, path, query) when is_ws_upgrade r && Handlers.valid_cell cell && Handlers.is_js cell
                                    && not (String.length path > 2 && String.sub path 0 3 = "/__") ->
        ws_upgrade t cell ~path ~query ~headers:r.headers
    | _ -> public t r >|= fun x -> H.Respond x

  (* The fleet TLS identity: a P-256 key and a self-signed certificate,
     created once, conditionally, by whichever node is first, like the fleet
     secret. Anyone who can read the bucket can already impersonate the
     fleet through that secret, so this adds no new trust in the bucket; it
     keeps peer traffic private and tamper-proof on the network. *)
  let rec peer_tls bucket =
    let load body =
      match Wire.of_string body with
      | Error e -> Error e
      | Ok j -> (
          match X509.Certificate.decode_pem (Wire.str_field "cert" j), X509.Private_key.decode_pem (Wire.str_field "key" j) with
          | Ok cert, Ok key -> Ok (cert, key)
          | Error (`Msg e), _ | _, Error (`Msg e) -> Error e)
    in
    B.get bucket Wire.peer_tls_key >>= function
    | Ok (Some (body, _)) -> ( match load body with Ok id -> Lwt.return id | Error e -> failwith ("bad " ^ Wire.peer_tls_key ^ ": " ^ e))
    | Ok None -> (
        let key = X509.Private_key.generate `P256 in
        let name = [ X509.Distinguished_name.(Relative_distinguished_name.singleton (CN (Common_name.v "celld-ml fleet"))) ] in
        let now = Mirage_ptime.now () in
        let until = Option.value (Ptime.add_span now (Ptime.Span.of_int_s (20 * 365 * 86400))) ~default:now in
        let cert =
          match X509.Signing_request.create name key with
          | Error (`Msg e) -> failwith e
          | Ok csr -> (
              match X509.Signing_request.sign csr ~valid_from:now ~valid_until:until key name with
              | Ok c -> c
              | Error e -> failwith (Fmt.str "%a" X509.Validation.pp_signature_error e))
        in
        let body = Wire.to_string (Wire.Obj [ ("cert", Wire.Str (X509.Certificate.encode_pem cert)); ("key", Wire.Str (X509.Private_key.encode_pem key)) ]) in
        B.put bucket ~guard:B.Absent Wire.peer_tls_key body >>= function
        | Ok (`Ok _) -> Log.info (fun m -> m "created the fleet TLS identity"); Lwt.return (cert, key)
        | _ -> peer_tls bucket)
    | Error f ->
        Log.warn (fun m -> m "cannot read the fleet TLS identity (%s); retrying" (string_of_failure (fail_of f)));
        sleep_ms 2_000 >>= fun () -> peer_tls bucket

  let serve_tls stack ~port config handler =
    S.TCP.listen (S.tcp stack) ~port (fun flow ->
        Tls_flow.server_of_flow config flow >>= function
        | Ok tls -> H.handle_conn_ext handler (tls_conn tls)
        | Error e ->
            Log.debug (fun m -> m "peer TLS handshake failed: %a" Tls_flow.pp_write_error e);
            S.TCP.close flow)

  let rec peer_secret bucket =
    B.get bucket Wire.peer_auth_key >>= function
    | Ok (Some (body, _)) -> ( match Wire.of_string body with Ok j -> Lwt.return (Wire.str_field "secret" j) | Error _ -> failwith "bad peer-auth.json")
    | Ok None -> (
        let secret = Wire.hex (Mirage_crypto_rng.generate 32) in
        B.put bucket ~guard:B.Absent Wire.peer_auth_key (Wire.to_string (Wire.Obj [ ("secret", Str secret) ])) >>= function
        | Ok (`Ok _) -> Lwt.return secret
        | _ -> peer_secret bucket)
    | Error f ->
        Log.warn (fun m -> m "cannot read the fleet secret from the bucket (%s); retrying" (string_of_failure (fail_of f)));
        sleep_ms 2_000 >>= fun () -> peer_secret bucket

  (* The secrets device: key=value lines, NUL-padded, at most 64 KiB. *)
  let read_secrets sec =
    Sec.get_info sec >>= fun info ->
    let sectors = min (Int64.to_int info.size_sectors) (65536 / info.sector_size) in
    if sectors <= 0 then Lwt.return []
    else
      let buf = Cstruct.create (sectors * info.sector_size) in
      Sec.read sec 0L [ buf ] >|= function
      | Error _ -> []
      | Ok () ->
          let s = Cstruct.to_string buf in
          let s = match String.index_opt s '\000' with Some i -> String.sub s 0 i | None -> s in
          List.filter_map
            (fun line ->
              match String.index_opt line '=' with
              | Some i -> Some (String.trim (String.sub line 0 i), String.trim (String.sub line (i + 1) (String.length line - i - 1)))
              | None -> None)
            (String.split_on_char '\n' s)

  let start stack blk sec =
    let me = node_id () in
    read_secrets sec >>= fun secrets ->
    let secret k = Option.value (List.assoc_opt k secrets) ~default:"" in
    D.open_ blk >>= fun (disk, records) ->
    let bucket, bucket_ep =
      let b = bucket_addr () in
      if String.length b > 5 && String.sub b 0 5 = "s3://" then begin
        let rest = String.sub b 5 (String.length b - 5) in
        let addr, name = match String.index_opt rest '/' with Some i -> (String.sub rest 0 i, String.sub rest (i + 1) (String.length rest - i - 1)) | None -> invalid_arg "--bucket=s3://ip:port/bucket" in
        let ip, port = parse_addr addr in
        let host = if s3_host () = "" then addr else s3_host () in
        let tls =
          if not (s3_tls ()) then None
          else
            match B.tls_config ?fingerprint:(if s3_tls_fingerprint () = "" then None else Some (s3_tls_fingerprint ())) ~host () with
            | Ok c -> Some c
            | Error e -> failwith ("TLS configuration: " ^ e)
        in
        if secret "s3_access_key" = "" || secret "s3_secret_key" = "" then
          Log.warn (fun m -> m "no S3 credentials on the secrets device (s3_access_key=..., s3_secret_key=...)");
        let creds = { Celld_s3.access_key = secret "s3_access_key"; secret_key = secret "s3_secret_key"; region = s3_region () } in
        (B.create ~host ?tls ~s3:{ B.bucket = name; creds } stack ip port, (ip, port))
      end
      else
        let ip, port = parse_addr b in
        (B.create stack ip port, (ip, port))
    in
    let session = random_hex 8 in
    Celld_sqlite.seed (Mirage_crypto_rng.generate 32);
    Celld_js.random_source := Mirage_crypto_rng.generate;
    Celld_js.logger :=
      (fun level msg ->
        match level with
        | "error" -> Log.err (fun m -> m "js: %s" msg)
        | "warn" -> Log.warn (fun m -> m "js: %s" msg)
        | "debug" -> Log.debug (fun m -> m "js: %s" msg)
        | _ -> Log.info (fun m -> m "js: %s" msg));
    let config = { default_config with ttl_ms = ttl_ms (); idle_ms = idle_ms () } in
    let core = Core.create ~config ~node:me ~session ~addr:(advertise ()) () in
    let stream, push = Lwt_stream.create () in
    let t =
      {
        stack; bucket; core; me; session;
        fleet = durability () = "fleet";
        secret = "";
        peer_client = None;
        mailbox = (fun ev -> push (Some ev));
        runtimes = Hashtbl.create 16;
        log = Queue.create ();
        next_seq = 1;
        active = [];
        want = [];
        peers = [];
        last_shed_ms = 0;
        alarm_timers = Hashtbl.create 16;
        pumping = Hashtbl.create 16;
        tunnels = Hashtbl.create 8;
        sockets = Hashtbl.create 16;
        cell_locks = Hashtbl.create 16;
        dns_cache = Hashtbl.create 16;
        tls_clients = Hashtbl.create 8;
        bucket_ep;
        class_versions = Hashtbl.create 8;
        loops = Hashtbl.create 16;
        workers = Hashtbl.create 4;
        cache = Hashtbl.create 64;
        cache_bytes = 0;
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
    peer_tls bucket >>= fun (cert, key) ->
    let server =
      match Tls.Config.server ~certificates:(`Single ([ cert ], key)) () with Ok c -> c | Error (`Msg e) -> failwith ("peer TLS server: " ^ e)
    in
    let fingerprint = X509.Certificate.fingerprint `SHA256 cert in
    let authenticator = X509.Authenticator.cert_fingerprint ~time:(fun () -> Some (Mirage_ptime.now ())) ~hash:`SHA256 ~fingerprint in
    t.peer_client <- (match Tls.Config.client ~authenticator () with Ok c -> Some c | Error (`Msg e) -> failwith ("peer TLS client: " ^ e));
    serve_tls stack ~port:internal_port server (internal_ext t);
    S.TCP.listen (S.tcp stack) ~port:public_port (fun flow -> H.handle_conn_ext (public_ext t) (H.tcp_conn flow));
    Lwt.async (fun () -> actor t stream);
    t.mailbox Start_node;
    Lwt.async (fun () ->
        let rec loop () =
          discover t >>= fun () ->
          if t.fleet then recruit t;
          rebalance_once t;
          scan_alarms t >>= fun () ->
          Hashtbl.iter (fun cell rt -> if rt.store.sql <> None then Lwt.async (fun () -> pump_outbox t cell)) t.runtimes;
          check_deployments t;
          shed_if_needed t;
          (* A cell with open sockets is in use, whether or not they talk. *)
          Hashtbl.iter
            (fun cell _ -> Lwt.async (fun () -> serve_cell t ~forwarded:true cell ~meth:"WS" ~path:"/__ws/touch" ~query:[] ~body:"" >|= ignore))
            t.sockets;
          sleep_ms 2_000 >>= loop
        in
        sleep_ms 500 >>= loop);
    S.listen stack
end
