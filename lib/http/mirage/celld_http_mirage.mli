(** HTTP/1.1 server and client over a MirageOS TCP/IP stack. *)

module Make (S : Tcpip.Stack.V4V6) : sig
  val serve :
    S.t -> port:int -> (Celld_http.request -> Celld_http.response Lwt.t) -> unit
  (** Listens on [port]. Each connection runs a keep-alive loop that handles
      pipelined requests in order; [Connection: close] from the client ends
      it after the reply. Handler exceptions become 500; unparseable input
      gets 400 and the connection is closed. *)

  (** A connected byte stream, so the client runs over TCP or TLS. *)
  type conn = {
    write : string -> (unit, string) result Lwt.t;
    read : unit -> [ `Data of string | `Eof | `Error of string ] Lwt.t;
    close : unit -> unit Lwt.t;
  }

  val tcp_connect : S.t -> Ipaddr.t -> int -> (conn, string) result Lwt.t

  val request_over :
    connect:(unit -> (conn, string) result Lwt.t) ->
    default_host:string ->
    ?timeout_ns:int64 ->
    Celld_http.request ->
    (Celld_http.response, [ `Connect of string | `Io of string | `Timeout ]) result Lwt.t
  (** Like {!request}, on a connection from [connect]; a failure inside
      [connect] is [`Connect]. *)

  val request :
    S.t ->
    ?timeout_ns:int64 ->
    Ipaddr.t ->
    int ->
    Celld_http.request ->
    ( Celld_http.response,
      [ `Connect of string | `Io of string | `Timeout ] )
    result
    Lwt.t
  (** One connection per request, sent with [Connection: close].
      [`Connect] means the request was certainly not sent (connect failed or
      timed out). [`Io] / [`Timeout] happen after the first byte may have
      been written, so the request may or may not have been applied.
      Default timeout 10 s, covering connect + exchange. A HEAD request's
      response is parsed without a body. *)
end
