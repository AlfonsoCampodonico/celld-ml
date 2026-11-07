(* mirage >= 4.11.0 & < 4.12.0 *)
open Mirage

let packages =
  [
    package "lwt" ~max:"6.0.0";
    package "cstruct";
    package "tcpip";
    package "mirage-flow";
    package "mirage-sleep";
    package "mirage-ptime";
    package "mirage-mtime";
    package "mirage-runtime";
    package "mirage-crypto-rng";
    package "mirage-block";
    package "digestif";
    package "cmdliner";
    package "logs";
    package "fmt";
    package "ipaddr";
  ]

let main = main ~packages "Unikernel.Make" (stackv4v6 @-> block @-> job)
let stack = generic_stackv4v6 default_network
let log = block_of_file "log"
let () = register "node" [ main $ stack $ log ]
