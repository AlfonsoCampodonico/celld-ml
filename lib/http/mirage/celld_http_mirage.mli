(** HTTP/1.1 server and client over a MirageOS TCP/IP stack. *)

module Make (S : Tcpip.Stack.V4V6) : sig
  (** A connected byte stream, so client and server run over TCP or TLS. *)
  type conn = {
    write : string -> (unit, string) result Lwt.t;
    read : unit -> [ `Data of string | `Eof | `Error of string ] Lwt.t;
    close : unit -> unit Lwt.t;
  }

  val tcp_conn : S.TCP.flow -> conn

  val handle_conn : (Celld_http.request -> Celld_http.response Lwt.t) -> conn -> unit Lwt.t
  (** The server loop of {!serve} on one connection. *)

  type reply = Respond of Celld_http.response | Upgrade of Celld_http.response * (conn -> string -> unit Lwt.t)

  val handle_conn_ext : ?initial:string -> (Celld_http.request -> reply Lwt.t) -> conn -> unit Lwt.t
  (** Like {!handle_conn}, but an [Upgrade] reply writes its response and
      hands the connection, with any bytes already read, to its function.
      [initial] is input that arrived before this loop took over. *)

  val exchange :
    conn ->
    ?timeout_ns:int64 ->
    Celld_http.request ->
    (Celld_http.response * string, [ `Fresh_eof | `Io of string | `Timeout ]) result Lwt.t
  (** One request on an open connection, kept open. Returns the response and
      any bytes past it. [`Fresh_eof]: the peer closed the connection before
      a byte of response. *)

  val serve :
    S.t -> port:int -> (Celld_http.request -> Celld_http.response Lwt.t) -> unit
  (** Listens on [port]. Each connection runs a keep-alive loop that handles
      pipelined requests in order; [Connection: close] from the client ends
      it after the reply. Handler exceptions become 500; unparseable input
      gets 400 and the connection is closed. *)

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
