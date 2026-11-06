(** Client for the bucket wire protocol (DESIGN.md, port 9000). *)

module Make (S : Tcpip.Stack.V4V6) : sig
  type t

  val create : S.t -> Ipaddr.t -> int -> t

  type failure =
    | Definite of string  (** certainly not applied *)
    | Ambiguous of string  (** may or may not have applied *)

  val get : t -> string -> ((string * string) option, failure) result Lwt.t
  (** [Some (body, etag)], or [None] on 404. Transient failures are retried
      twice. *)

  type guard = Absent | Match of string
  (** [Absent] sends [If-None-Match: *]; [Match e] sends [If-Match: "e"]. *)

  val put :
    t ->
    ?guard:guard ->
    string ->
    string ->
    ([ `Ok of string | `Precondition_failed ], failure) result Lwt.t
  (** [put t ?guard key body]; [`Ok etag]. Never retried: 5xx and errors
      after the request may have been sent are [Ambiguous]. *)

  val delete :
    t ->
    ?guard:guard ->
    string ->
    ([ `Ok | `Not_found | `Precondition_failed ], failure) result Lwt.t
  (** Same failure rules as [put]. *)

  val list : t -> prefix:string -> (string list, failure) result Lwt.t
  (** Sorted keys under [prefix]. Retried like [get]. *)
end
