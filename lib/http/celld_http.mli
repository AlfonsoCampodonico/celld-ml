(** Pure HTTP/1.1 parsing and serialization over strings. *)

type request = {
  meth : string;
  target : string;
  headers : (string * string) list;
  body : string;
}

type response = {
  status : int;
  headers : (string * string) list;
  body : string;
}

val header : string -> (string * string) list -> string option
(** [header name headers] is the first value of [name], compared
    case-insensitively. *)

type 'a parse =
  | Done of 'a * int  (** value and bytes consumed *)
  | Partial  (** need more input *)
  | Error of string

val parse_request : string -> request parse
(** Parses one request from the start of the buffer. Bodies are delimited by
    Content-Length or Transfer-Encoding: chunked (decoded; the returned
    headers drop Transfer-Encoding). No body otherwise. *)

val parse_response : ?head:bool -> string -> response parse
(** As [parse_request]. [~head:true] means the response answers a HEAD
    request and carries no body regardless of Content-Length. A response
    with neither Content-Length nor chunked encoding (and a status that may
    have a body) is [Partial] until the caller knows the connection closed;
    see [parse_response_eof]. *)

val parse_response_eof : ?head:bool -> string -> response parse
(** Like [parse_response] but the input is known to be complete (the peer
    closed): a body without framing extends to the end of input. *)

val serialize_request : request -> string
(** Adds Content-Length (replacing any given one, dropping
    Transfer-Encoding) and Host (["localhost"]) if missing. *)

val serialize_response : response -> string
(** Adds the reason phrase and Content-Length (except for 1xx/204/304). *)

val reason : int -> string

val path_and_query : string -> string * (string * string) list
(** Splits a target into its path (not decoded) and its percent-decoded
    query parameters (['+'] decodes to a space). *)

val pct_decode : string -> string
(** Decodes [%XX] escapes; malformed escapes are kept verbatim. *)

val pct_encode : string -> string
(** Keeps [A-Za-z0-9._~/-] and percent-encodes everything else. *)

val expected_length : string -> int option
(** If the buffer holds a complete head whose body is framed by
    Content-Length, the total message length (head + body). Lets a reader
    skip re-parsing until that many bytes have arrived. *)
