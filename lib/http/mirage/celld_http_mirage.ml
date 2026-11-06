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

  (* Per-connection loop. [buf] holds bytes received but not yet consumed. *)
  let handle_flow handler flow =
    let buf = Buffer.create 4096 in
    (* Minimum buffer length before it is worth re-parsing. *)
    let need = ref 0 in
    let close () = T.close flow in
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
          write_string flow (Celld_http.serialize_response resp) >>= ( function
          | Error e ->
              Log.debug (fun m -> m "write failed: %s" e);
              close ()
          | Ok () -> if closing then close () else loop () )
      | Celld_http.Error e ->
          Log.debug (fun m -> m "bad request: %s" e);
          let resp =
            { Celld_http.status = 400; headers = [ ("Connection", "close") ]; body = e ^ "\n" }
          in
          write_string flow (Celld_http.serialize_response resp) >>= fun _ -> close ()
      | Celld_http.Partial ->
          (match Celld_http.expected_length contents with
          | Some n -> need := n
          | None -> need := 0);
          read_more ()
    and read_more () =
          T.read flow >>= function
          | Ok (`Data cs) ->
              Buffer.add_string buf (Cstruct.to_string cs);
              loop ()
          | Ok `Eof -> close ()
          | Error e ->
              Log.debug (fun m -> m "read failed: %a" T.pp_error e);
              close ()
    in
    Lwt.catch loop (fun exn ->
        Log.err (fun m -> m "connection loop raised: %s" (Printexc.to_string exn));
        close ())

  let serve s ~port handler = T.listen (S.tcp s) ~port (handle_flow handler)

  let request s ?(timeout_ns = 10_000_000_000L) ip port (req : Celld_http.request) =
    let is_head = String.uppercase_ascii req.meth = "HEAD" in
    let req =
      let hs =
        List.filter
          (fun (k, _) -> String.lowercase_ascii k <> "connection")
          req.headers
      in
      let hs =
        if Celld_http.header "host" hs = None then
          ("Host", Ipaddr.to_string ip ^ ":" ^ string_of_int port) :: hs
        else hs
      in
      { req with headers = hs @ [ ("Connection", "close") ] }
    in
    let sent = ref false in
    let flow_ref = ref None in
    let exchange () =
      T.create_connection (S.tcp s) (ip, port) >>= function
      | Error e -> Lwt.return (Error (`Connect (Fmt.str "%a" T.pp_error e)))
      | Ok flow -> (
          flow_ref := Some flow;
          sent := true;
          write_string flow (Celld_http.serialize_request req) >>= function
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
                      (match Celld_http.expected_length contents with
                      | Some n -> need := n
                      | None -> need := 0);
                    read_more ()
              and read_more () =
                    T.read flow >>= function
                    | Ok (`Data cs) ->
                        Buffer.add_string buf (Cstruct.to_string cs);
                        read_loop ()
                    | Ok `Eof -> (
                        match Celld_http.parse_response_eof ~head:is_head (Buffer.contents buf) with
                        | Celld_http.Done (resp, _) -> Lwt.return (Ok resp)
                        | Celld_http.Partial -> Lwt.return (Error (`Io "eof before complete response"))
                        | Celld_http.Error e -> Lwt.return (Error (`Io ("parse: " ^ e))))
                    | Error e -> Lwt.return (Error (`Io (Fmt.str "read: %a" T.pp_error e)))
              in
              read_loop ())
    in
    let timeout () =
      Mirage_sleep.ns timeout_ns >|= fun () ->
      if !sent then Error `Timeout else Error (`Connect "connect timeout")
    in
    Lwt.catch
      (fun () -> Lwt.pick [ exchange (); timeout () ])
      (fun exn ->
        let m = Printexc.to_string exn in
        Lwt.return (if !sent then Error (`Io m) else Error (`Connect m)))
    >>= fun result ->
    (match !flow_ref with
    | Some flow -> Lwt.async (fun () -> Lwt.catch (fun () -> T.close flow) (fun _ -> Lwt.return_unit))
    | None -> ());
    Lwt.return result
end
