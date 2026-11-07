(* The vocabulary the core and its executors share. Like celld's
   crates/logic/types.rs: the core never performs I/O and never reads a
   clock. Every completion comes back as an [event] carrying the [op] it
   answers, and a completion for an op the core no longer waits on is
   ignored. *)

type node_id = string
type cell_id = string
type epoch = int
type op = int
type req = int

(* An [Ambiguous] failure may or may not have taken effect. A conditional write
   that fails ambiguously is re-read, never blindly retried. *)
type failure = Definite of string | Ambiguous of string

type guard = Absent | Match of string

(* cells/<cell>/own.json. [o_node = None] is a released record. *)
type owner_record = { o_node : node_id option; o_epoch : epoch; o_etag : string }

type log_state = Log_none | Log_open | Log_recovering | Log_sealed

type peer = { p_node : node_id; p_addr : string }

(* nodes/<node>.json: the node lease, with the node log's record folded in as
   celld does. [l_etag] is ignored on writes. *)
type lease = {
  l_node : node_id;
  l_session : string;
  l_addr : string;
  l_expires_ms : int;
  l_protocol : int;
  l_log : log_state;
  l_followers : peer list;
  l_cells : int;  (** resident cells, for rebalancing *)
  l_draining : bool;  (** not a follower or handoff target *)
  l_etag : string;
}

type cas_result = Applied of string | Rejected | Cas_failed of failure

type route = Local of epoch | Remote of peer

type request_error =
  | Node_unavailable
  | Node_fenced
  | Resolve_failed of string
  | Acquire_failed of string
  | Restore_failed of string
  | Peer_incompatible
  | Durability_unproven of string

type proof = Fleet | Bucket

type timer = Lease_renew | Lease_fence | Idle_sweep

type event =
  (* node lease and authority *)
  | Start_node
  | Self_lease_read of { op : op; result : (lease option, failure) result }
  | Own_log_recovered of { op : op; result : (string, failure) result }
      (** The previous incarnation's log has been tiered and its record sealed.
          Carries the record's new etag. *)
  | Lease_cas_completed of { op : op; result : cas_result }
  | Followers_chosen of peer list
  | Timer_fired of { timer : timer; generation : int }
  (* moving cells between nodes *)
  | Shed of { cells : cell_id list; target : peer }
      (** Hand these resident cells to [target] once they go quiet. *)
  | Drain of peer list
      (** Stop taking ownership, hand every cell to these peers, then retire
          the lease and exit. *)
  | Drain_checked of { op : op; result : (unit, failure) result }
      (** Every capture of this node is in the bucket. *)
  (* ingress *)
  | Request of { req : req; cell : cell_id }
  | Invalidate_remote of cell_id
  (* ownership resolution *)
  | Owner_read of { op : op; cell : cell_id; result : (owner_record option, failure) result }
  | Lease_read of { op : op; cell : cell_id; result : (lease option, failure) result }
  | Log_recovered of { op : op; cell : cell_id; result : (unit, failure) result }
  | Owner_cas_completed of { op : op; cell : cell_id; result : cas_result }
  (* activation *)
  | Restore_completed of { op : op; cell : cell_id; result : (unit, string) result }
  (* output gate *)
  | Output_at of { req : req; position : int option }
      (** A local request's response is ready. [position] is the commit it
          wrote, if any. *)
  | Durable_reached of { op : op; result : (int, failure) result; source : proof }
  | Ownership_verified of { op : op; result : (bool, failure) result }
  (* eviction *)
  | Durability_checked of { op : op; cell : cell_id; result : (unit, failure) result }
  | Runtime_stopped of { op : op; cell : cell_id }
  | Owner_released of { op : op; cell : cell_id; result : cas_result }

type eff =
  | Schedule_timer of { timer : timer; generation : int; at_ms : int }
  | Read_self_lease of { op : op }
  | Recover_own_log of { op : op; lease : lease }
  | Cas_lease of { op : op; guard : guard; lease : lease }
  | Activate_followers of peer list
      (** The lease now names these followers, so the executor may ship to
          them, starting with the backlog that is not yet in the bucket. *)
  | Halt of string
  | Check_drained of { op : op }
  | Exit_node of string  (** a clean exit: the lease is already retired *)
  | Hand_off of { cell : cell_id; peer : peer }
      (** Ask [peer] to activate a cell this node just released to it. *)
  | Read_owner of { op : op; cell : cell_id }
  | Read_lease of { op : op; cell : cell_id; node : node_id }
  | Recover_log of { op : op; cell : cell_id; lease : lease }
  | Cas_owner of { op : op; cell : cell_id; guard : guard; epoch : epoch }
  | Restore of { op : op; cell : cell_id; epoch : epoch; takeover : bool }
  | Complete of { req : req; result : (route, request_error) result }
  | Await_durable of { op : op; cell : cell_id; epoch : epoch; position : int }
  | Verify_ownership of { op : op; cell : cell_id; epoch : epoch }
  | Release of { req : req; result : (unit, request_error) result }
  | Ensure_durable of { op : op; cell : cell_id; epoch : epoch }
  | Stop_runtime of { op : op; cell : cell_id; epoch : epoch; reset : bool }
  | Release_owner of { op : op; cell : cell_id; guard : guard; epoch : epoch }

type config = {
  ttl_ms : int;  (** node lease lifetime; renewed every ttl/3 *)
  idle_ms : int;  (** a resident cell idle this long is evicted *)
  sweep_ms : int;
  protocol : int;
  max_reconciles : int;
}

let default_config =
  { ttl_ms = 10_000; idle_ms = 30_000; sweep_ms = 1_000; protocol = 5; max_reconciles = 3 }

let string_of_failure = function Definite s -> "definite: " ^ s | Ambiguous s -> "ambiguous: " ^ s

let string_of_request_error = function
  | Node_unavailable -> "node unavailable"
  | Node_fenced -> "node fenced"
  | Resolve_failed s -> "resolve failed: " ^ s
  | Acquire_failed s -> "acquire failed: " ^ s
  | Restore_failed s -> "restore failed: " ^ s
  | Peer_incompatible -> "peer protocol incompatible"
  | Durability_unproven s -> "durability unproven: " ^ s

let string_of_log_state = function
  | Log_none -> "none"
  | Log_open -> "open"
  | Log_recovering -> "recovering"
  | Log_sealed -> "sealed"

let log_state_of_string = function
  | "open" -> Log_open
  | "recovering" -> Log_recovering
  | "sealed" -> Log_sealed
  | _ -> Log_none
