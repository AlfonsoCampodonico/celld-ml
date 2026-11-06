open Lwt.Infix

module Make (S : Tcpip.Stack.V4V6) = struct
  module H = Celld_http_mirage.Make (S)

  type t = { stack : S.t; ip : Ipaddr.t; port : int }

  let create stack ip port = { stack; ip; port }

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

  let send t ?(timeout_ns = 10_000_000_000L) meth target headers body =
    H.request t.stack ~timeout_ns t.ip t.port
      { Celld_http.meth; target; headers; body }

  let obj_target key = "/o/" ^ Celld_http.pct_encode key

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
        send t "GET" (obj_target key) [] "" >|= function
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
    with_retries (fun () ->
        send t "GET" ("/list?prefix=" ^ Celld_http.pct_encode prefix) [] "" >|= function
        | Ok r when r.status = 200 ->
            `Done (String.split_on_char '\n' r.body |> List.filter (fun s -> s <> ""))
        | Ok r when r.status >= 500 -> `Transient (status_msg r)
        | Ok r -> `Fatal (status_msg r)
        | Error e -> `Transient (err_string e))

  (* Writes: classify a transport failure by whether the request left. *)
  let write_failure = function
    | `Connect m -> Definite ("connect: " ^ m)
    | e -> Ambiguous (err_string e)

  let put t ?guard key body =
    send t "PUT" (obj_target key) (guard_headers guard) body >|= function
    | Ok r when r.status = 200 || r.status = 201 || r.status = 204 -> (
        match Celld_http.header "etag" r.headers with
        | Some e -> Ok (`Ok (unquote e))
        | None -> Error (Ambiguous "PUT succeeded without ETag"))
    | Ok r when r.status = 412 -> Ok `Precondition_failed
    | Ok r when r.status >= 500 -> Error (Ambiguous (status_msg r))
    | Ok r -> Error (Definite (status_msg r))
    | Error e -> Error (write_failure e)

  let delete t ?guard key =
    send t "DELETE" (obj_target key) (guard_headers guard) "" >|= function
    | Ok r when r.status = 200 || r.status = 204 -> Ok `Ok
    | Ok r when r.status = 404 -> Ok `Not_found
    | Ok r when r.status = 412 -> Ok `Precondition_failed
    | Ok r when r.status >= 500 -> Error (Ambiguous (status_msg r))
    | Ok r -> Error (Definite (status_msg r))
    | Error e -> Error (write_failure e)
end
