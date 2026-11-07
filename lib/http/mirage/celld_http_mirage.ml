let src = Logs.Src.create "celld.http" ~doc:"celld HTTP over tcpip"

module Log = (val Logs.src_log src : Logs.LOG)

open Lwt.Infix

module Make (S : Tcpip.Stack.V4V6) = struct
  module T = S.TCP

  let write_string flow s =
    T.write flow (Cstruct.of_string s) >|= function
    | Ok () -> Ok ()
    | Error e -> Error (Fmt.str "%a" T.pp_write_error e)

  let wants_close (r : Celld_http.request) =
    match Celld_http.header "connection" r.headers with
    | Some v -> String.lowercase_ascii (String.trim v) = "close"
    | None -> false

  let internal_error msg =
    { Celld_http.status = 500; headers = [ ("Content-Type", "text/plain") ]; body = msg ^ "\n" }

  (* A connected byte stream: plain TCP here, TLS in the bucket client. *)
  type conn = {
    write : string -> (unit, string) result Lwt.t;
    read : unit -> [ `Data of string | `Eof | `Error of string ] Lwt.t;
    close : unit -> unit Lwt.t;
  }

  let tcp_conn flow =
    {
      write = write_string flow;
      read =
        (fun () ->
          T.read flow >|= function
          | Ok (`Data cs) -> `Data (Cstruct.to_string cs)
          | Ok `Eof -> `Eof
          | Error e -> `Error (Fmt.str "%a" T.pp_error e));
      close = (fun () -> T.close flow);
    }

  (* Per-connection loop. [buf] holds bytes received but not yet consumed. *)
  let handle_conn handler conn =
    let buf = Buffer.create 4096 in
    (* Minimum buffer length before it is worth re-parsing. *)
    let need = ref 0 in
    let close () = conn.close () in
    let rec loop () =
      if Buffer.length buf = 0 || Buffer.length buf < !need then read_more ()
      else
      let contents = Buffer.contents buf in
      match Celld_http.parse_request contents with
      | Celld_http.Done (req, used) ->
          let rest = String.sub contents used (String.length contents - used) in
          Buffer.clear buf;
          Buffer.add_string buf rest;
          need := 0;
          Lwt.catch
            (fun () -> handler req)
            (fun exn ->
              Log.err (fun m -> m "handler raised: %s" (Printexc.to_string exn));
              Lwt.return (internal_error (Printexc.to_string exn)))
          >>= fun resp ->
          let closing = wants_close req in
          let resp =
            if closing then
              { resp with Celld_http.headers = ("Connection", "close") :: resp.headers }
            else resp
          in
          conn.write (Celld_http.serialize_response resp) >>= ( function
          | Error e ->
              Log.debug (fun m -> m "write failed: %s" e);
              close ()
          | Ok () -> if closing then close () else loop () )
      | Celld_http.Error e ->
          Log.debug (fun m -> m "bad request: %s" e);
          let resp =
            { Celld_http.status = 400; headers = [ ("Connection", "close") ]; body = e ^ "\n" }
          in
          conn.write (Celld_http.serialize_response resp) >>= fun _ -> close ()
      | Celld_http.Partial ->
          (match Celld_http.expected_length contents with
          | Some n -> need := n
          | None -> need := 0);
          read_more ()
    and read_more () =
          conn.read () >>= function
          | `Data d ->
              Buffer.add_string buf d;
              loop ()
          | `Eof -> close ()
          | `Error e ->
              Log.debug (fun m -> m "read failed: %s" e);
              close ()
    in
    Lwt.catch loop (fun exn ->
        Log.err (fun m -> m "connection loop raised: %s" (Printexc.to_string exn));
        close ())

  let serve s ~port handler = T.listen (S.tcp s) ~port (fun flow -> handle_conn handler (tcp_conn flow))

  let tcp_connect s ip port =
    T.create_connection (S.tcp s) (ip, port) >|= function
    | Error e -> Error (Fmt.str "%a" T.pp_error e)
    | Ok flow -> Ok (tcp_conn flow)

  (* One request on a fresh connection from [connect]. A failure before the
     request could have left is [`Connect]; after, [`Io] or [`Timeout]. *)
  let request_over ~connect ~default_host ?(timeout_ns = 10_000_000_000L) (req : Celld_http.request) =
    let is_head = String.uppercase_ascii req.meth = "HEAD" in
    let req =
      let hs = List.filter (fun (k, _) -> String.lowercase_ascii k <> "connection") req.headers in
      let hs = if Celld_http.header "host" hs = None then ("Host", default_host) :: hs else hs in
      { req with headers = hs @ [ ("Connection", "close") ] }
    in
    let sent = ref false in
    let conn_ref = ref None in
    let exchange () =
      connect () >>= function
      | Error e -> Lwt.return (Error (`Connect e))
      | Ok conn -> (
          conn_ref := Some conn;
          sent := true;
          conn.write (Celld_http.serialize_request req) >>= function
          | Error e -> Lwt.return (Error (`Io ("write: " ^ e)))
          | Ok () ->
              let buf = Buffer.create 4096 in
              let need = ref 0 in
              let rec read_loop () =
                if Buffer.length buf = 0 || Buffer.length buf < !need then read_more ()
                else
                  let contents = Buffer.contents buf in
                  match Celld_http.parse_response ~head:is_head contents with
                  | Celld_http.Done (resp, _) -> Lwt.return (Ok resp)
                  | Celld_http.Error e -> Lwt.return (Error (`Io ("parse: " ^ e)))
                  | Celld_http.Partial ->
                      if not is_head then
                        (match Celld_http.expected_length contents with Some n -> need := n | None -> need := 0);
                      read_more ()
              and read_more () =
                conn.read () >>= function
                | `Data d ->
                    Buffer.add_string buf d;
                    read_loop ()
                | `Eof -> (
                    match Celld_http.parse_response_eof ~head:is_head (Buffer.contents buf) with
                    | Celld_http.Done (resp, _) -> Lwt.return (Ok resp)
                    | Celld_http.Partial -> Lwt.return (Error (`Io "eof before complete response"))
                    | Celld_http.Error e -> Lwt.return (Error (`Io ("parse: " ^ e))))
                | `Error e -> Lwt.return (Error (`Io ("read: " ^ e)))
              in
              read_loop ())
    in
    let timeout () =
      Mirage_sleep.ns timeout_ns >|= fun () -> if !sent then Error `Timeout else Error (`Connect "connect timeout")
    in
    Lwt.catch
      (fun () -> Lwt.pick [ exchange (); timeout () ])
      (fun exn ->
        let m = Printexc.to_string exn in
        Lwt.return (if !sent then Error (`Io m) else Error (`Connect m)))
    >>= fun result ->
    (match !conn_ref with
    | Some c -> Lwt.async (fun () -> Lwt.catch c.close (fun _ -> Lwt.return_unit))
    | None -> ());
    Lwt.return result

  let request s ?timeout_ns ip port req =
    request_over ~connect:(fun () -> tcp_connect s ip port)
      ~default_host:(Ipaddr.to_string ip ^ ":" ^ string_of_int port) ?timeout_ns req
end
