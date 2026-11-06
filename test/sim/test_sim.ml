(* A deterministic simulator: several cores, one in-memory bucket, and a
   synchronous executor. It checks the protocol, not the I/O. Like celld's
   simulator, it feeds the same [on_event] the unikernel does. *)

open Celld_logic

type obj = Owner of node_id option * epoch | Lease_obj of lease

let bucket : (string, obj * string) Hashtbl.t = Hashtbl.create 16
let etag_n = ref 0

let fresh_etag () =
  incr etag_n;
  Printf.sprintf "t%d" !etag_n

let cas key guard v =
  match guard, Hashtbl.find_opt bucket key with
  | Absent, None ->
      let e = fresh_etag () in
      Hashtbl.replace bucket key (v, e);
      Applied e
  | Match m, Some (_, e) when m = e ->
      let e = fresh_etag () in
      Hashtbl.replace bucket key (v, e);
      Applied e
  | _ -> Rejected

type node = {
  st : Core.t;
  mutable alive : bool;
  mutable timers : (timer * int * int) list;
  mutable completed : (req * (route, request_error) result) list;
  mutable released : (req * (unit, request_error) result) list;
  mutable halted : string option;
  mutable ambiguous_next_owner_cas : bool;
}

let now = ref 0
let nodes : (string, node) Hashtbl.t = Hashtbl.create 4

let mk name =
  let st = Core.create ~node:name ~session:(name ^ "-s" ^ string_of_int !now) ~addr:(name ^ ":8081") () in
  let n = { st; alive = true; timers = []; completed = []; released = []; halted = None; ambiguous_next_owner_cas = false } in
  Hashtbl.replace nodes name n;
  n

let own_key c = "cells/" ^ c ^ "/own.json"
let lease_key n = "nodes/" ^ n ^ ".json"

let rec feed n ev = if n.alive then List.iter (exec n) (Core.on_event n.st ~now_ms:!now ev)

and exec n (fx : eff) =
  let me = n.st.Core.node in
  match fx with
  | Schedule_timer { timer; generation; at_ms } -> n.timers <- (timer, generation, at_ms) :: n.timers
  | Read_self_lease { op } ->
      let result = match Hashtbl.find_opt bucket (lease_key me) with Some (Lease_obj l, e) -> Some { l with l_etag = e } | _ -> None in
      feed n (Self_lease_read { op; result = Ok result })
  | Recover_own_log { op; lease } ->
      let r = cas (lease_key me) (Match lease.l_etag) (Lease_obj { lease with l_log = Log_sealed }) in
      feed n (Own_log_recovered { op; result = (match r with Applied e -> Ok e | _ -> Error (Definite "seal")) })
  | Cas_lease { op; guard; lease } -> feed n (Lease_cas_completed { op; result = cas (lease_key me) guard (Lease_obj lease) })
  | Activate_followers _ -> ()
  | Halt reason -> n.halted <- Some reason
  | Read_owner { op; cell } ->
      let result =
        match Hashtbl.find_opt bucket (own_key cell) with
        | Some (Owner (node, epoch), e) -> Some { o_node = node; o_epoch = epoch; o_etag = e }
        | _ -> None
      in
      feed n (Owner_read { op; cell; result = Ok result })
  | Read_lease { op; cell; node } ->
      let result = match Hashtbl.find_opt bucket (lease_key node) with Some (Lease_obj l, e) -> Some { l with l_etag = e } | _ -> None in
      feed n (Lease_read { op; cell; result = Ok result })
  | Recover_log { op; cell; lease } ->
      let r = cas (lease_key lease.l_node) (Match lease.l_etag) (Lease_obj { lease with l_log = Log_sealed }) in
      feed n (Log_recovered { op; cell; result = (match r with Applied _ -> Ok () | _ -> Error (Definite "seal")) })
  | Cas_owner { op; cell; guard; epoch } ->
      let r = cas (own_key cell) guard (Owner (Some me, epoch)) in
      let r = if n.ambiguous_next_owner_cas then (n.ambiguous_next_owner_cas <- false; Cas_failed (Ambiguous "timeout")) else r in
      feed n (Owner_cas_completed { op; cell; result = r })
  | Restore { op; cell; _ } -> feed n (Restore_completed { op; cell; result = Ok () })
  | Complete { req; result } -> n.completed <- (req, result) :: n.completed
  | Await_durable { op; position; _ } -> feed n (Durable_reached { op; result = Ok position; source = Bucket })
  | Verify_ownership { op; cell; epoch } ->
      let ok = match Hashtbl.find_opt bucket (own_key cell) with Some (Owner (Some x, e), _) -> x = me && e = epoch | _ -> false in
      feed n (Ownership_verified { op; result = Ok ok })
  | Release { req; result } -> n.released <- (req, result) :: n.released
  | Ensure_durable { op; cell; _ } -> feed n (Durability_checked { op; cell; result = Ok () })
  | Stop_runtime { op; cell; _ } -> feed n (Runtime_stopped { op; cell })
  | Release_owner { op; cell; guard; epoch } ->
      feed n (Owner_released { op; cell; result = cas (own_key cell) guard (Owner (None, epoch)) })

(* Advance the clock, firing due timers in time order. *)
let advance ms =
  let target = !now + ms in
  let rec loop () =
    let due =
      Hashtbl.fold
        (fun _ n acc ->
          if not n.alive then acc
          else List.fold_left (fun acc (t, g, at) -> if at <= target then (at, n, t, g) :: acc else acc) acc n.timers)
        nodes []
      |> List.sort (fun (a, _, _, _) (b, _, _, _) -> compare a b)
    in
    match due with
    | [] -> now := target
    | (at, n, t, g) :: _ ->
        now := max !now at;
        n.timers <- List.filter (fun (t', g', at') -> not (t' = t && g' = g && at' = at)) n.timers;
        feed n (Timer_fired { timer = t; generation = g });
        loop ()
  in
  loop ()

let req_n = ref 0

let request n cell =
  incr req_n;
  let r = !req_n in
  feed n (Request { req = r; cell });
  match List.assoc_opt r n.completed with Some res -> (r, res) | None -> failwith "request did not complete"

let pass = ref 0

let check name cond =
  if not cond then (Printf.printf "FAIL %s\n" name; exit 1);
  incr pass;
  Printf.printf "ok   %s\n" name

let owner cell = match Hashtbl.find_opt bucket (own_key cell) with Some (Owner (n, e), _) -> (n, e) | _ -> (None, 0)

let () =
  let a = mk "a" and b = mk "b" in
  feed a Start_node;
  feed b Start_node;
  check "both nodes hold leases" (Core.lease a.st <> None && Core.lease b.st <> None);

  (* First activation claims epoch 1 with If-None-Match. *)
  let r, res = request a "Counter:x" in
  check "a serves Counter:x locally at e1" (res = Ok (Local 1));
  check "own.json names a at e1" (owner "Counter:x" = (Some "a", 1));
  feed a (Output_at { req = r; position = Some 1 });
  check "write released after bucket proof + ownership re-read" (List.assoc_opt r a.released = Some (Ok ()));

  (* Another node routes to the live owner. *)
  let _, res = request b "Counter:x" in
  check "b routes Counter:x to a" (match res with Ok (Remote p) -> p.p_node = "a" | _ -> false);

  (* Read-only output trails an open barrier. *)
  let w, _ = request a "Counter:x" in
  let rd, _ = request a "Counter:x" in
  (* Hold the proof: intercept by not executing Await_durable synchronously. *)
  let fx = Core.on_event a.st ~now_ms:!now (Output_at { req = w; position = Some 2 }) in
  let await_op = match fx with [ Await_durable { op; _ } ] -> op | _ -> failwith "expected await" in
  feed a (Output_at { req = rd; position = None });
  check "read held behind unproven write" (List.assoc_opt rd a.released = None);
  feed a (Durable_reached { op = await_op; result = Ok 2; source = Fleet });
  check "fleet proof releases write and trailing read together"
    (List.assoc_opt w a.released = Some (Ok ()) && List.assoc_opt rd a.released = Some (Ok ()));

  (* Crash a: its lease stays in the bucket with its log open, then expires. *)
  (match Hashtbl.find_opt bucket (lease_key "a") with
  | Some (Lease_obj l, e) -> Hashtbl.replace bucket (lease_key "a") (Lease_obj { l with l_log = Log_open }, e)
  | _ -> ());
  a.alive <- false;
  b.st.Core.cells |> Hashtbl.iter (fun _ c -> c.Core.phase <- Core.Inactive) (* drop b's route cache *);
  let _, res = request b "Counter:x" in
  check "b still routes to a while a's lease is live" (match res with Ok (Remote _) -> true | _ -> false);
  advance 11_000;
  b.st.Core.cells |> Hashtbl.iter (fun _ c -> c.Core.phase <- Core.Inactive);
  let _, res = request b "Counter:x" in
  check "b takes over after a's lease expires, at e2" (res = Ok (Local 2));
  check "a's open log was sealed before takeover"
    (match Hashtbl.find_opt bucket (lease_key "a") with Some (Lease_obj l, _) -> l.l_log = Log_sealed | _ -> false);
  check "own.json names b at e2" (owner "Counter:x" = (Some "b", 2));

  (* a comes back from a partition still thinking it owns the cell: the
     ownership re-read refuses its bucket proof. *)
  a.alive <- true;
  let stale_req = 999 in
  Hashtbl.replace a.st.Core.requests stale_req "Counter:x";
  (* its lease has expired, so it self-fences before any output *)
  advance 0;
  feed a (Timer_fired { timer = Lease_fence; generation = a.st.Core.lease_gen });
  check "stale a self-fences" (a.halted <> None);
  feed a (Output_at { req = stale_req; position = Some 3 });
  check "fenced node releases nothing" (List.assoc_opt stale_req a.released = Some (Error Node_fenced));

  (* A restarted node with the same id recovers its previous incarnation's
     log before taking a lease. *)
  (match Hashtbl.find_opt bucket (lease_key "b") with
  | Some (Lease_obj l, e) -> Hashtbl.replace bucket (lease_key "b") (Lease_obj { l with l_log = Log_open }, e)
  | _ -> ());
  Hashtbl.remove nodes "b";
  let b2 = mk "b" in
  feed b2 Start_node;
  check "restarted b holds a fresh lease" (match Core.lease b2.st with Some l -> l.l_session = b2.st.Core.session && l.l_log = Log_none | None -> false);
  let r, res = request b2 "Counter:x" in
  check "restarted b reclaims its own cell at e3" (res = Ok (Local 3));
  feed b2 (Output_at { req = r; position = None });

  (* Ambiguous claim that actually landed is adopted by re-reading. *)
  b2.ambiguous_next_owner_cas <- true;
  let r, res = request b2 "Counter:y" in
  feed b2 (Output_at { req = r; position = None });
  check "ambiguous CAS reconciled to the epoch it claimed" (res = Ok (Local 1) && owner "Counter:y" = (Some "b", 1));

  (* Idle eviction releases ownership; the next request reactivates at e+1. *)
  advance 31_000;
  check "idle cell released" (owner "Counter:y" = (None, 1));
  let r, res = request b2 "Counter:y" in
  feed b2 (Output_at { req = r; position = None });
  check "released cell reactivates at e2" (res = Ok (Local 2));

  (* Leases keep renewing across many ttl periods. *)
  advance 60_000;
  check "b's lease is still live after renewals" (b2.halted = None && match Core.lease b2.st with Some l -> l.l_expires_ms > !now | None -> false);

  (* A failed proof fails the write and resets the cell. *)
  let w, wres = request b2 "Counter:y" in
  let we = match wres with Ok (Local e) -> e | _ -> failwith "expected local" in
  let fx = Core.on_event b2.st ~now_ms:!now (Output_at { req = w; position = Some 9 }) in
  let op = match fx with [ Await_durable { op; _ } ] -> op | _ -> failwith "expected await" in
  feed b2 (Durable_reached { op; result = Error (Definite "upload failed"); source = Bucket });
  check "unproven write is not acknowledged" (match List.assoc_opt w b2.released with Some (Error (Durability_unproven _)) -> true | _ -> false);
  let _, res = request b2 "Counter:y" in
  check "reset cell restores from the bucket at a new epoch" (res = Ok (Local (we + 1)));

  Printf.printf "%d checks passed\n" !pass
