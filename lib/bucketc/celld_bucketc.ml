(* The bucket client: the celld-ml bucket unikernel's protocol, or any
   S3-compatible store with conditional writes (If-None-Match: * and
   If-Match on PUT), over plain TCP or TLS. *)

open Lwt.Infix

module Make (S : Tcpip.Stack.V4V6) = struct
  module H = Celld_http_mirage.Make (S)
  module Tls_flow = Tls_mirage.Make (S.TCP)

  type s3 = { bucket : string; creds : Celld_s3.credentials }

  type t = {
    stack : S.t;
    ip : Ipaddr.t;
    port : int;
    host : string;  (** Host header, and the TLS server name *)
    tls : Tls.Config.client option;
    s3 : s3 option;
  }

  let create ?host ?tls ?s3 stack ip port =
    { stack; ip; port; host = Option.value host ~default:(Ipaddr.to_string ip ^ ":" ^ string_of_int port); tls; s3 }

  (* Certificates: a pinned SHA-256 certificate fingerprint (hex), or the
     Mozilla NSS root store. *)
  let tls_config ?fingerprint ~host () =
    let time () = Some (Mirage_ptime.now ()) in
    let authenticator =
      match fingerprint with
      | Some hex ->
          let hex = String.concat "" (String.split_on_char ':' (String.lowercase_ascii hex)) in
          let raw = String.init (String.length hex / 2) (fun i -> Char.chr (int_of_string ("0x" ^ String.sub hex (2 * i) 2))) in
          Ok (X509.Authenticator.cert_fingerprint ~time ~hash:`SHA256 ~fingerprint:raw)
      | None -> Ca_certs_nss.authenticator ()
    in
    match authenticator with
    | Error (`Msg m) -> Error m
    | Ok authenticator ->
        let peer_name = match Domain_name.of_string host with Ok d -> Result.to_option (Domain_name.host d) | Error _ -> None in
        Result.map_error (fun (`Msg m) -> m) (Tls.Config.client ~authenticator ?peer_name ())

  let connect t () =
    match t.tls with
    | None -> H.tcp_connect t.stack t.ip t.port
    | Some config -> (
        S.TCP.create_connection (S.tcp t.stack) (t.ip, t.port) >>= function
        | Error e -> Lwt.return (Error (Fmt.str "%a" S.TCP.pp_error e))
        | Ok flow -> (
            let host = match Domain_name.of_string (List.hd (String.split_on_char ':' t.host)) with Ok d -> Result.to_option (Domain_name.host d) | Error _ -> None in
            Tls_flow.client_of_flow config ?host flow >|= function
            | Error e -> Error (Fmt.str "tls: %a" Tls_flow.pp_write_error e)
            | Ok tls ->
                Ok
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
                  }))

  type failure = Definite of string | Ambiguous of string
  type guard = Absent | Match of string

  let unquote s =
    let s = String.trim s in
    let n = String.length s in
    if n >= 2 && s.[0] = '"' && s.[n - 1] = '"' then String.sub s 1 (n - 2) else s

  let guard_headers = function
    | None -> []
    | Some Absent -> [ ("If-None-Match", "*") ]
    | Some (Match e) -> [ ("If-Match", "\"" ^ e ^ "\"") ]

  (* [path] is the object key, or None for the bucket itself; [query] is
     unencoded. Native: /o/<key> and /list; S3: path-style, SigV4-signed. *)
  let send t ?(timeout_ns = 10_000_000_000L) meth ?key ?(query = []) headers body =
    let qs q = String.concat "&" (List.map (fun (k, v) -> Celld_s3.uri_encode k ^ "=" ^ Celld_s3.uri_encode v) q) in
    let target, headers =
      match t.s3 with
      | None ->
          let path = match key with Some k -> "/o/" ^ Celld_http.pct_encode k | None -> "/list" in
          ((if query = [] then path else path ^ "?" ^ qs query), headers)
      | Some s3 ->
          let path = "/" ^ s3.bucket ^ match key with Some k -> "/" ^ Celld_s3.uri_encode ~slash:true k | None -> "" in
          let amz_date = Celld_s3.amz_date_of_unix (int_of_float (Ptime.to_float_s (Mirage_ptime.now ()))) in
          let headers =
            Celld_s3.sign s3.creds ~amz_date ~meth ~host:t.host ~path ~query ~headers ~payload_sha256:(Celld_s3.sha256_hex body)
          in
          ((if query = [] then path else path ^ "?" ^ qs query), headers)
    in
    H.request_over ~connect:(connect t) ~default_host:t.host ~timeout_ns { Celld_http.meth; target; headers; body }

  let err_string = function
    | `Connect m -> "connect: " ^ m
    | `Io m -> "io: " ^ m
    | `Timeout -> "timeout"

  (* Reads: everything is retryable and a failure is never ambiguous. *)
  let with_retries ?(attempts = 3) f =
    let rec go n =
      f () >>= function
      | `Done v -> Lwt.return (Ok v)
      | `Fatal m -> Lwt.return (Error (Definite m))
      | `Transient m ->
          if n <= 1 then Lwt.return (Error (Definite m))
          else Mirage_sleep.ns 100_000_000L >>= fun () -> go (n - 1)
    in
    go attempts

  let status_msg (r : Celld_http.response) =
    Printf.sprintf "status %d: %s" r.status (String.trim r.body)

  let get t key =
    with_retries (fun () ->
        send t "GET" ~key [] "" >|= function
        | Ok r when r.status = 200 ->
            let etag =
              match Celld_http.header "etag" r.headers with Some e -> unquote e | None -> ""
            in
            `Done (Some (r.body, etag))
        | Ok r when r.status = 404 -> `Done None
        | Ok r when r.status >= 500 -> `Transient (status_msg r)
        | Ok r -> `Fatal (status_msg r)
        | Error e -> `Transient (err_string e))

  let list t ~prefix =
    match t.s3 with
    | None ->
        with_retries (fun () ->
            send t "GET" ~query:[ ("prefix", prefix) ] [] "" >|= function
            | Ok r when r.status = 200 ->
                `Done (String.split_on_char '\n' r.body |> List.filter (fun s -> s <> ""))
            | Ok r when r.status >= 500 -> `Transient (status_msg r)
            | Ok r -> `Fatal (status_msg r)
            | Error e -> `Transient (err_string e))
    | Some _ ->
        (* ListObjectsV2, following continuation tokens. *)
        let rec pages token acc =
          with_retries (fun () ->
              let query = [ ("list-type", "2"); ("prefix", prefix) ] @ match token with Some tk -> [ ("continuation-token", tk) ] | None -> [] in
              send t "GET" ~query [] "" >|= function
              | Ok r when r.status = 200 -> `Done (Celld_s3.parse_list r.body)
              | Ok r when r.status >= 500 -> `Transient (status_msg r)
              | Ok r -> `Fatal (status_msg r)
              | Error e -> `Transient (err_string e))
          >>= function
          | Error _ as e -> Lwt.return e
          | Ok { keys; next = Some tk } -> pages (Some tk) (acc @ keys)
          | Ok { keys; next = None } -> Lwt.return (Ok (acc @ keys))
        in
        pages None []

  (* Writes: classify a transport failure by whether the request left. *)
  let write_failure = function
    | `Connect m -> Definite ("connect: " ^ m)
    | e -> Ambiguous (err_string e)

  let put t ?guard key body =
    send t "PUT" ~key (guard_headers guard) body >|= function
    | Ok r when r.status = 200 || r.status = 201 || r.status = 204 -> (
        match Celld_http.header "etag" r.headers with
        | Some e -> Ok (`Ok (unquote e))
        | None -> Error (Ambiguous "PUT succeeded without ETag"))
    (* S3 answers 409 when a concurrent conditional write wins the race:
       this one did not apply, the same as a failed precondition. *)
    | Ok r when r.status = 412 || r.status = 409 -> Ok `Precondition_failed
    | Ok r when r.status >= 500 -> Error (Ambiguous (status_msg r))
    | Ok r -> Error (Definite (status_msg r))
    | Error e -> Error (write_failure e)

  let delete t ?guard key =
    send t "DELETE" ~key (guard_headers guard) "" >|= function
    | Ok r when r.status = 200 || r.status = 204 -> Ok `Ok
    | Ok r when r.status = 404 -> Ok `Not_found
    | Ok r when r.status = 412 -> Ok `Precondition_failed
    | Ok r when r.status >= 500 -> Error (Ambiguous (status_msg r))
    | Ok r -> Error (Definite (status_msg r))
    | Error e -> Error (write_failure e)
end
