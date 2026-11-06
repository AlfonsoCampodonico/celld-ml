(* The core: [on_event] is the only way behavioral state advances (celld's
   crates/logic/lib.rs). The executor and the simulator both call it from
   one serial loop and perform the effects it returns. *)

open Types

type claim = { guard : guard; epoch : epoch; takeover : bool }

type phase =
  | Inactive
  | Reading_owner of { op : op; reconciling : (claim * int) option }
  | Reading_lease of { op : op; owner : owner_record }
  | Recovering_log of { op : op; owner : owner_record }
  | Acquiring of { op : op; claim : claim; tries : int }
  | Restoring of { op : op; epoch : epoch; etag : string }
  | Resident of { epoch : epoch; etag : string }
  | Remote of { peer : peer; epoch : epoch; until_ms : int }
  | Ensuring of { op : op; epoch : epoch; etag : string }
  | Cleaning of { op : op; epoch : epoch; etag : string; reset : bool }
  | Releasing of { op : op; epoch : epoch }

type cell = {
  id : cell_id;
  mutable phase : phase;
  mutable waiters : req list;  (** newest first *)
  mutable active : int;
  mutable last_used_ms : int;
  mutable barriers : op list;  (** open write barriers, newest first *)
}

type barrier = {
  b_cell : cell_id;
  b_epoch : epoch;
  b_position : int;
  b_owner : req;
  mutable b_followers : req list;
  mutable b_verifying : bool;
}

type authority =
  | Unstarted
  | Reading of { op : op; held : lease option }
  | Recovering_own of { op : op; prior : lease }
  | Writing of { op : op; lease : lease; held : lease option }
  | Held of lease
  | Fenced

type t = {
  node : node_id;
  session : string;
  addr : string;
  config : config;
  mutable now_ms : int;
  mutable next_op : op;
  mutable authority : authority;
  mutable lease_gen : int;
  mutable want_followers : peer list;
  cells : (cell_id, cell) Hashtbl.t;
  requests : (req, cell_id) Hashtbl.t;  (** local requests awaiting [Output_at] *)
  barriers : (op, barrier) Hashtbl.t;
}

let create ?(config = default_config) ~node ~session ~addr () =
  {
    node;
    session;
    addr;
    config;
    now_ms = 0;
    next_op = 1;
    authority = Unstarted;
    lease_gen = 0;
    want_followers = [];
    cells = Hashtbl.create 64;
    requests = Hashtbl.create 64;
    barriers = Hashtbl.create 64;
  }

let fresh_op st =
  let op = st.next_op in
  st.next_op <- op + 1;
  op

let cell st id =
  match Hashtbl.find_opt st.cells id with
  | Some c -> c
  | None ->
      let c = { id; phase = Inactive; waiters = []; active = 0; last_used_ms = st.now_ms; barriers = [] } in
      Hashtbl.replace st.cells id c;
      c

(* ---- node authority ---- *)

let held_lease st =
  match st.authority with
  | Held l | Writing { held = Some l; _ } | Reading { held = Some l; _ } -> Some l
  | _ -> None

(* The node may serve and release output only while a lease it wrote is
   unexpired. A pending renewal keeps the old lease's authority. *)
let authority_valid st =
  match held_lease st with Some l -> st.now_ms < l.l_expires_ms | None -> false

let fenced st = st.authority = Fenced

let new_lease st ~log ~followers =
  {
    l_node = st.node;
    l_session = st.session;
    l_addr = st.addr;
    l_expires_ms = st.now_ms + st.config.ttl_ms;
    l_protocol = st.config.protocol;
    l_log = log;
    l_followers = followers;
    l_etag = "";
  }

let write_lease st ~guard ~held lease =
  let op = fresh_op st in
  st.authority <- Writing { op; lease; held };
  [ Cas_lease { op; guard; lease } ]

let halt st reason =
  st.authority <- Fenced;
  [ Halt reason ]

let renew st (l : lease) =
  let followers = st.want_followers in
  let log = if followers = [] then l.l_log else Log_open in
  write_lease st ~guard:(Match l.l_etag) ~held:(Some l) (new_lease st ~log ~followers)

let lease_held st (l : lease) =
  let previous = held_lease st in
  st.authority <- Held l;
  st.lease_gen <- st.lease_gen + 1;
  let gen = st.lease_gen in
  let timers =
    [
      Schedule_timer { timer = Lease_renew; generation = gen; at_ms = st.now_ms + (st.config.ttl_ms / 3) };
      Schedule_timer { timer = Lease_fence; generation = gen; at_ms = l.l_expires_ms };
    ]
  in
  let first = if previous = None then [ Schedule_timer { timer = Idle_sweep; generation = 0; at_ms = st.now_ms + st.config.sweep_ms } ] else [] in
  let activate =
    match previous with
    | Some p when p.l_followers = l.l_followers -> []
    | _ -> if l.l_followers = [] then [] else [ Activate_followers l.l_followers ]
  in
  (* A follower change that arrived while this write was in flight. *)
  let again = if l.l_followers <> st.want_followers then renew st l else [] in
  timers @ first @ activate @ again

let on_self_lease_read st op result =
  match st.authority with
  | Reading { op = o; held } when o = op -> (
      match result with
      | Error _ -> (
          match held with
          | Some l -> lease_held st l (* retry on the next renew tick *)
          | None -> halt st "cannot read own node lease")
      | Ok None -> (
          match held with
          | Some _ -> halt st "node lease missing"
          | None -> write_lease st ~guard:Absent ~held:None (new_lease st ~log:Log_none ~followers:[]))
      | Ok (Some r) when r.l_session = st.session ->
          (* An ambiguous write of ours that did land. *)
          if r.l_expires_ms > st.now_ms then lease_held st r else halt st "node lease expired"
      | Ok (Some r) -> (
          match held with
          | Some _ -> halt st "node lease replaced"
          | None ->
              if r.l_log = Log_open || r.l_log = Log_recovering then begin
                (* A previous incarnation of this node may hold acknowledged
                   writes on its followers that never reached the bucket. *)
                let op = fresh_op st in
                st.authority <- Recovering_own { op; prior = r };
                [ Recover_own_log { op; lease = r } ]
              end
              else write_lease st ~guard:(Match r.l_etag) ~held:None (new_lease st ~log:Log_none ~followers:[])))
  | _ -> []

let on_lease_cas st op result =
  match st.authority with
  | Writing { op = o; lease; held } when o = op -> (
      match result with
      | Applied etag -> lease_held st { lease with l_etag = etag }
      | Rejected -> halt st "node lease mismatch"
      | Cas_failed (Ambiguous _) ->
          let op = fresh_op st in
          st.authority <- Reading { op; held };
          [ Read_self_lease { op } ]
      | Cas_failed (Definite e) -> (
          match held with Some l -> lease_held st l | None -> halt st ("cannot write node lease: " ^ e)))
  | _ -> []

let on_own_log_recovered st op result =
  match st.authority with
  | Recovering_own { op = o; _ } when o = op -> (
      match result with
      | Ok etag -> write_lease st ~guard:(Match etag) ~held:None (new_lease st ~log:Log_none ~followers:[])
      | Error e -> halt st ("cannot recover previous log: " ^ string_of_failure e))
  | _ -> []

let on_followers_chosen st peers =
  st.want_followers <- peers;
  match st.authority with
  | Held l when l.l_followers <> peers -> renew st l
  | _ -> [] (* picked up when the in-flight write lands *)

(* ---- cells: resolution and activation ---- *)

let fail_waiters st c err =
  ignore st;
  let reqs = List.rev c.waiters in
  c.waiters <- [];
  c.phase <- Inactive;
  List.map (fun req -> Complete { req; result = Error err }) reqs

let admit st c epoch req =
  c.active <- c.active + 1;
  c.last_used_ms <- st.now_ms;
  Hashtbl.replace st.requests req c.id;
  Complete { req; result = Ok (Local epoch) }

let complete_waiters st c =
  let reqs = List.rev c.waiters in
  c.waiters <- [];
  match c.phase with
  | Resident { epoch; _ } -> List.map (admit st c epoch) reqs
  | Remote { peer; _ } -> List.map (fun req -> Complete { req; result = Ok (Remote peer) }) reqs
  | _ -> assert false

let start_resolve st c =
  let op = fresh_op st in
  c.phase <- Reading_owner { op; reconciling = None };
  [ Read_owner { op; cell = c.id } ]

let acquire st c ?(tries = 0) claim =
  let op = fresh_op st in
  c.phase <- Acquiring { op; claim; tries };
  [ Cas_owner { op; cell = c.id; guard = claim.guard; epoch = claim.epoch } ]

let begin_restore st c ~epoch ~etag ~takeover =
  let op = fresh_op st in
  c.phase <- Restoring { op; epoch; etag };
  [ Restore { op; cell = c.id; epoch; takeover } ]

let on_request st req id =
  if fenced st then [ Complete { req; result = Error Node_fenced } ]
  else if not (authority_valid st) then [ Complete { req; result = Error Node_unavailable } ]
  else
    let c = cell st id in
    c.last_used_ms <- st.now_ms;
    match c.phase with
    | Resident { epoch; _ } -> [ admit st c epoch req ]
    | Ensuring { epoch; etag; _ } ->
        (* A request cancels an eviction that has not yet stopped the
           runtime. The pending [Durability_checked] no longer matches. *)
        c.phase <- Resident { epoch; etag };
        [ admit st c epoch req ]
    | Remote { peer; until_ms; _ } when st.now_ms < until_ms -> [ Complete { req; result = Ok (Remote peer) } ]
    | Remote _ | Inactive ->
        c.waiters <- req :: c.waiters;
        start_resolve st c
    | _ ->
        c.waiters <- req :: c.waiters;
        []

let on_owner_read st op id result =
  match Hashtbl.find_opt st.cells id with
  | Some ({ phase = Reading_owner { op = o; reconciling }; _ } as c) when o = op -> (
      let tries = match reconciling with Some (_, n) -> n | None -> 0 in
      match result with
      | Error e -> fail_waiters st c (Resolve_failed (string_of_failure e))
      | Ok None -> acquire st c ~tries { guard = Absent; epoch = 1; takeover = false }
      | Ok (Some r) -> (
          match r.o_node with
          | None -> acquire st c ~tries { guard = Match r.o_etag; epoch = r.o_epoch + 1; takeover = false }
          | Some n when n = st.node -> (
              match reconciling with
              | Some (claim, _) when claim.epoch = r.o_epoch ->
                  (* The ambiguous claim landed. *)
                  begin_restore st c ~epoch:r.o_epoch ~etag:r.o_etag ~takeover:claim.takeover
              | _ ->
                  (* Ours from an earlier activation: every activation advances
                     the epoch, so no epoch ever has two writers. *)
                  acquire st c ~tries { guard = Match r.o_etag; epoch = r.o_epoch + 1; takeover = false })
          | Some n ->
              let op = fresh_op st in
              c.phase <- Reading_lease { op; owner = r };
              [ Read_lease { op; cell = id; node = n } ]))
  | _ -> []

let takeover st c (owner : owner_record) =
  acquire st c { guard = Match owner.o_etag; epoch = owner.o_epoch + 1; takeover = true }

let on_lease_read st op id result =
  match Hashtbl.find_opt st.cells id with
  | Some ({ phase = Reading_lease { op = o; owner }; _ } as c) when o = op -> (
      match result with
      | Error e -> fail_waiters st c (Resolve_failed (string_of_failure e))
      | Ok (Some l) when l.l_expires_ms > st.now_ms ->
          if l.l_protocol <> st.config.protocol then
            (* Incompatibility never authorizes a takeover. *)
            fail_waiters st c Peer_incompatible
          else begin
            c.phase <- Remote { peer = { p_node = l.l_node; p_addr = l.l_addr }; epoch = owner.o_epoch; until_ms = l.l_expires_ms };
            complete_waiters st c
          end
      | Ok (Some l) when l.l_log = Log_open || l.l_log = Log_recovering ->
          (* The takeover gate: the dead owner may have acknowledged writes that
             only its followers hold. *)
          let op = fresh_op st in
          c.phase <- Recovering_log { op; owner };
          [ Recover_log { op; cell = id; lease = l } ]
      | Ok _ -> takeover st c owner)
  | _ -> []

let on_log_recovered st op id result =
  match Hashtbl.find_opt st.cells id with
  | Some ({ phase = Recovering_log { op = o; owner }; _ } as c) when o = op -> (
      match result with
      | Ok () -> takeover st c owner
      | Error e -> fail_waiters st c (Resolve_failed ("owner log recovery: " ^ string_of_failure e)))
  | _ -> []

let on_owner_cas st op id result =
  match Hashtbl.find_opt st.cells id with
  | Some ({ phase = Acquiring { op = o; claim; tries }; _ } as c) when o = op -> (
      match result with
      | Applied etag -> begin_restore st c ~epoch:claim.epoch ~etag ~takeover:claim.takeover
      | Rejected ->
          if tries >= st.config.max_reconciles then fail_waiters st c (Acquire_failed "contended")
          else
            let op = fresh_op st in
            c.phase <- Reading_owner { op; reconciling = Some ({ claim with epoch = -1 }, tries + 1) };
            [ Read_owner { op; cell = id } ]
      | Cas_failed (Definite e) -> fail_waiters st c (Acquire_failed e)
      | Cas_failed (Ambiguous _) ->
          if tries >= st.config.max_reconciles then fail_waiters st c (Acquire_failed "ambiguous")
          else
            let op = fresh_op st in
            c.phase <- Reading_owner { op; reconciling = Some (claim, tries + 1) };
            [ Read_owner { op; cell = id } ])
  | _ -> []

let on_restore_completed st op id result =
  match Hashtbl.find_opt st.cells id with
  | Some ({ phase = Restoring { op = o; epoch; etag }; _ } as c) when o = op -> (
      match result with
      | Ok () ->
          c.phase <- Resident { epoch; etag };
          complete_waiters st c
      | Error e ->
          (* We still own the record. The next request reclaims at epoch+1. *)
          fail_waiters st c (Restore_failed e))
  | _ -> []

let on_invalidate_remote st id =
  (match Hashtbl.find_opt st.cells id with
  | Some ({ phase = Remote _; _ } as c) -> c.phase <- Inactive
  | _ -> ());
  []

(* ---- output gate ---- *)

let release_all_of (b : barrier) result =
  List.map (fun req -> Release { req; result }) (b.b_owner :: List.rev b.b_followers)

let rec settle st op result =
  match Hashtbl.find_opt st.barriers op with
  | None -> []
  | Some b -> (
      Hashtbl.remove st.barriers op;
      let c = cell st b.b_cell in
      c.barriers <- List.filter (( <> ) op) c.barriers;
      let released = release_all_of b result in
      match result with
      | Ok () -> released
      | Error _ ->
          (* An unproven write may have been seen by nobody, but the runtime's
             state is ahead of what the bucket can prove: reset to the bucket.
             Later barriers on this cell are built on it, so they fail too. *)
          let later = List.concat_map (fun op -> settle st op result) c.barriers in
          let reset =
            match c.phase with
            | Resident { epoch; etag } | Ensuring { epoch; etag; _ } when epoch = b.b_epoch ->
                let op = fresh_op st in
                c.phase <- Cleaning { op; epoch; etag; reset = true };
                [ Stop_runtime { op; cell = c.id; epoch; reset = true } ]
            | _ -> []
          in
          released @ later @ reset)

let on_output st req position =
  match Hashtbl.find_opt st.requests req with
  | None -> [ Release { req; result = Error (Durability_unproven "unknown request") } ]
  | Some id -> (
      Hashtbl.remove st.requests req;
      let c = cell st id in
      c.active <- max 0 (c.active - 1);
      c.last_used_ms <- st.now_ms;
      if not (authority_valid st) then [ Release { req; result = Error Node_fenced } ]
      else
        match c.phase, position with
        | Resident { epoch; _ }, Some position ->
            let op = fresh_op st in
            Hashtbl.replace st.barriers op
              { b_cell = id; b_epoch = epoch; b_position = position; b_owner = req; b_followers = []; b_verifying = false };
            c.barriers <- op :: c.barriers;
            [ Await_durable { op; cell = id; epoch; position } ]
        | Resident _, None -> (
            match c.barriers with
            | newest :: _ ->
                (* A read may have observed the newest unproven write: it trails
                   that write's proof and shares its verdict. *)
                let b = Hashtbl.find st.barriers newest in
                b.b_followers <- req :: b.b_followers;
                []
            | [] -> [ Release { req; result = Ok () } ])
        | _ -> [ Release { req; result = Error (Durability_unproven "cell reset before output") } ])

let on_durable st op result source =
  match Hashtbl.find_opt st.barriers op with
  | Some b when not b.b_verifying -> (
      match result with
      | Ok durable when durable >= b.b_position -> (
          match source with
          | Fleet -> settle st op (Ok ())
          | Bucket ->
              (* A bucket proof also needs the ownership record to still name
                 this node at this epoch. *)
              b.b_verifying <- true;
              [ Verify_ownership { op; cell = b.b_cell; epoch = b.b_epoch } ])
      | Ok _ -> settle st op (Error (Durability_unproven "proof below position"))
      | Error e -> settle st op (Error (Durability_unproven (string_of_failure e))))
  | _ -> []

let on_verified st op result =
  match Hashtbl.find_opt st.barriers op with
  | Some b when b.b_verifying -> (
      match result with
      | Ok true -> settle st op (Ok ())
      | Ok false -> settle st op (Error (Durability_unproven "ownership lost"))
      | Error e -> settle st op (Error (Durability_unproven (string_of_failure e))))
  | _ -> []

(* ---- eviction ---- *)

let sweep st =
  let fx =
    Hashtbl.fold
      (fun _ c acc ->
        match c.phase with
        | Resident { epoch; etag }
          when c.active = 0 && c.barriers = [] && st.now_ms - c.last_used_ms >= st.config.idle_ms ->
            let op = fresh_op st in
            c.phase <- Ensuring { op; epoch; etag };
            Ensure_durable { op; cell = c.id; epoch } :: acc
        | _ -> acc)
      st.cells []
  in
  Schedule_timer { timer = Idle_sweep; generation = 0; at_ms = st.now_ms + st.config.sweep_ms } :: fx

let on_durability_checked st op id result =
  match Hashtbl.find_opt st.cells id with
  | Some ({ phase = Ensuring { op = o; epoch; etag }; _ } as c) when o = op -> (
      match result with
      | Ok () when c.active = 0 ->
          let op = fresh_op st in
          c.phase <- Cleaning { op; epoch; etag; reset = false };
          [ Stop_runtime { op; cell = id; epoch; reset = false } ]
      | _ ->
          c.phase <- Resident { epoch; etag };
          [])
  | _ -> []

let on_runtime_stopped st op id =
  match Hashtbl.find_opt st.cells id with
  | Some ({ phase = Cleaning { op = o; epoch; etag; reset }; _ } as c) when o = op ->
      if reset then begin
        c.phase <- Inactive;
        if c.waiters <> [] then start_resolve st c else []
      end
      else
        let op = fresh_op st in
        c.phase <- Releasing { op; epoch };
        [ Release_owner { op; cell = id; guard = Match etag; epoch } ]
  | _ -> []

let on_owner_released st op id _result =
  match Hashtbl.find_opt st.cells id with
  | Some ({ phase = Releasing { op = o; _ }; _ } as c) when o = op ->
      (* Rejected or ambiguous both end here: the next activation re-reads. *)
      c.phase <- Inactive;
      if c.waiters <> [] then start_resolve st c else []
  | _ -> []

(* ---- timers ---- *)

let on_timer st timer generation =
  match timer with
  | Idle_sweep -> if fenced st then [] else sweep st
  | Lease_renew -> (
      match st.authority with Held l when generation = st.lease_gen -> renew st l | _ -> [])
  | Lease_fence -> (
      match held_lease st with
      | Some l when generation = st.lease_gen && st.now_ms >= l.l_expires_ms -> halt st "node lease expired"
      | _ -> [])

(* ---- entry point ---- *)

let on_event st ~now_ms event =
  st.now_ms <- max st.now_ms now_ms;
  if fenced st then
    match event with
    | Request { req; _ } -> [ Complete { req; result = Error Node_fenced } ]
    | Output_at { req; _ } -> [ Release { req; result = Error Node_fenced } ]
    | _ -> []
  else
    match event with
    | Start_node -> (
        match st.authority with
        | Unstarted ->
            let op = fresh_op st in
            st.authority <- Reading { op; held = None };
            [ Read_self_lease { op } ]
        | _ -> [])
    | Self_lease_read { op; result } -> on_self_lease_read st op result
    | Own_log_recovered { op; result } -> on_own_log_recovered st op result
    | Lease_cas_completed { op; result } -> on_lease_cas st op result
    | Followers_chosen peers -> on_followers_chosen st peers
    | Timer_fired { timer; generation } -> on_timer st timer generation
    | Request { req; cell } -> on_request st req cell
    | Invalidate_remote id -> on_invalidate_remote st id
    | Owner_read { op; cell; result } -> on_owner_read st op cell result
    | Lease_read { op; cell; result } -> on_lease_read st op cell result
    | Log_recovered { op; cell; result } -> on_log_recovered st op cell result
    | Owner_cas_completed { op; cell; result } -> on_owner_cas st op cell result
    | Restore_completed { op; cell; result } -> on_restore_completed st op cell result
    | Output_at { req; position } -> on_output st req position
    | Durable_reached { op; result; source } -> on_durable st op result source
    | Ownership_verified { op; result } -> on_verified st op result
    | Durability_checked { op; cell; result } -> on_durability_checked st op cell result
    | Runtime_stopped { op; cell } -> on_runtime_stopped st op cell
    | Owner_released { op; cell; result } -> on_owner_released st op cell result

(* ---- introspection for /state ---- *)

let string_of_phase = function
  | Inactive -> "inactive"
  | Reading_owner _ -> "reading-owner"
  | Reading_lease _ -> "reading-lease"
  | Recovering_log _ -> "recovering-owner-log"
  | Acquiring _ -> "acquiring"
  | Restoring { epoch; _ } -> Printf.sprintf "restoring(e%d)" epoch
  | Resident { epoch; _ } -> Printf.sprintf "resident(e%d)" epoch
  | Remote { peer; epoch; _ } -> Printf.sprintf "remote(%s,e%d)" peer.p_node epoch
  | Ensuring { epoch; _ } -> Printf.sprintf "ensuring-durability(e%d)" epoch
  | Cleaning { epoch; reset; _ } -> Printf.sprintf "cleaning(e%d%s)" epoch (if reset then ",reset" else "")
  | Releasing { epoch; _ } -> Printf.sprintf "releasing(e%d)" epoch

let cells st =
  Hashtbl.fold (fun id c acc -> (id, string_of_phase c.phase, c.active, List.length c.barriers) :: acc) st.cells []
  |> List.sort compare

let lease st = held_lease st
