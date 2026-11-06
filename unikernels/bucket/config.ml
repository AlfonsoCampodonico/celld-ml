(* mirage >= 4.11.0 & < 4.12.0 *)
open Mirage

let packages =
  [
    package "lwt" ~max:"6.0.0";
    package "cstruct";
    package "tcpip";
    package "mirage-flow";
    package "mirage-sleep";
    package "mirage-block";
    package "mirage-runtime";
    package "cmdliner";
    package "logs";
    package "fmt";
    package "ipaddr";
  ]

let main =
  main ~packages "Unikernel.Make" (stackv4v6 @-> block @-> job)

let stack = generic_stackv4v6 default_network
let storage = block_of_file "storage"
let () = register "bucket" [ main $ stack $ storage ]
