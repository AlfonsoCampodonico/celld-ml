(** Bucket client: the celld-ml bucket's wire protocol (DESIGN.md, port
    9000), or an S3-compatible store with conditional writes, over TCP or
    TLS. *)

module Make (S : Tcpip.Stack.V4V6) : sig
  type t

  type s3 = { bucket : string; creds : Celld_s3.credentials }

  val tls_config : ?fingerprint:string -> host:string -> unit -> (Tls.Config.client, string) result
  (** Verify the server by a pinned SHA-256 certificate fingerprint (hex,
      colons allowed), or else against the Mozilla NSS root store. *)

  val create : ?host:string -> ?tls:Tls.Config.client -> ?s3:s3 -> S.t -> Ipaddr.t -> int -> t
  (** [host] is the Host header and TLS server name (default ip:port).
      With [s3], requests are path-style and SigV4-signed. *)

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
