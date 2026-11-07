open Celld_cell

let check name c = if not c then (Printf.printf "FAIL %s\n" name; exit 1) else Printf.printf "ok   %s\n" name

let () =
  let bucket = Hashtbl.create 16 in
  let put epoch file =
    match Celld_ltx.decode file with
    | Ok f -> Hashtbl.replace bucket (Celld_ltx.key ~epoch ~level:0 ~min_txid:f.header.min_txid ~max_txid:f.header.max_txid) file
    | Error e -> failwith e
  in
  let load () =
    let keys = Hashtbl.fold (fun k _ acc -> k :: acc) bucket [] in
    let chain = Store.plan keys in
    Store.restore (List.map (fun k -> match Celld_ltx.decode (Hashtbl.find bucket k) with Ok f -> f | Error e -> failwith e) chain)
  in
  (* epoch 1: three counter writes plus a large value spanning pages *)
  let s = Store.empty () in
  for _ = 1 to 3 do
    let r = Handlers.counter s ~meth:"POST" ~path:"/increment" ~query:[] ~body:"" in
    assert r.wrote;
    let _, f = Store.commit s in
    put 1 f
  done;
  s.kv <- Store.M.add "big" (String.make 10_000 'x') s.kv;
  let tx, f = Store.commit s in
  put 1 f;
  check "first commit is a snapshot, txids advance" (tx = 4L && Hashtbl.mem bucket (Celld_ltx.key ~epoch:1 ~level:0 ~min_txid:1L ~max_txid:1L));
  (match load () with
  | Ok r -> check "restore replays the epoch-1 chain" (r.txid = 4L && Store.M.find "value" r.kv = "3" && String.length (Store.M.find "big" r.kv) = 10_000)
  | Error e -> check ("restore: " ^ e) false);
  (* epoch 2 opens with a snapshot of what it restored, then writes *)
  let s2 = match load () with Ok r -> r | Error e -> failwith e in
  put 2 (Store.snapshot s2);
  s2.kv <- Store.M.remove "big" s2.kv;
  let _, f = Store.commit s2 in
  put 2 f;
  (* a stale epoch-1 owner keeps writing into its superseded prefix *)
  s.kv <- Store.M.add "value" "999" s.kv;
  let _, f = Store.commit s in
  put 1 f;
  (match load () with
  | Ok r -> check "newest epoch wins; stale e1 write is fenced out; shrink applied" (r.txid = 5L && Store.M.find "value" r.kv = "3" && not (Store.M.mem "big" r.kv))
  | Error e -> check ("restore: " ^ e) false);
  (* a gap ends the chain *)
  let s3 = match load () with Ok r -> r | Error e -> failwith e in
  let _, _lost = Store.commit s3 in
  s3.kv <- Store.M.add "value" "7" s3.kv;
  let _, f = Store.commit s3 in
  put 2 f;
  (match load () with
  | Ok r -> check "restore stops at a txid gap" (r.txid = 5L)
  | Error e -> check ("restore: " ^ e) false);
  let r = Handlers.kv s2 ~meth:"PUT" ~path:"/a" ~query:[] ~body:"1" in
  check "kv handler writes" (r.wrote && (Handlers.kv s2 ~meth:"GET" ~path:"/a" ~query:[] ~body:"").body = "1");
  check "cell ids validated" (Handlers.valid_cell "Counter:x" && not (Handlers.valid_cell "a/b") && not (Handlers.valid_cell ".."))

(* Compaction + GC: a level-1 snapshot lets the files it covers and every
   older epoch go, and restore still lands on the same state. *)
let () =
  let bucket = Hashtbl.create 16 in
  let put ?(level = 0) epoch file =
    match Celld_ltx.decode file with
    | Ok f -> Hashtbl.replace bucket (Celld_ltx.key ~epoch ~level ~min_txid:f.header.min_txid ~max_txid:f.header.max_txid) file
    | Error e -> failwith e
  in
  let keys () = Hashtbl.fold (fun k _ acc -> k :: acc) bucket [] in
  let load () =
    Store.restore (List.map (fun k -> match Celld_ltx.decode (Hashtbl.find bucket k) with Ok f -> f | Error e -> failwith e) (Store.plan (keys ())))
  in
  let s = Store.empty () in
  let bump epoch =
    ignore (Handlers.counter s ~meth:"POST" ~path:"/increment" ~query:[] ~body:"");
    let _, f = Store.commit s in
    put epoch f
  in
  for _ = 1 to 3 do bump 1 done;
  (* epoch 2 opens with a snapshot of txid 3, then 5 more writes *)
  put 2 (Store.snapshot s);
  for _ = 1 to 5 do bump 2 done;
  put ~level:1 2 (Store.snapshot s);
  bump 2;
  let before = List.length (keys ()) in
  let dead = Store.garbage (keys ()) ~epoch:2 ~upto:8 in
  List.iter (Hashtbl.remove bucket) dead;
  check "gc drops older epoch and covered files" (List.length dead = 9 && before - List.length (keys ()) = 9);
  check "gc keeps the snapshot and what follows" (List.length (keys ()) = 2);
  (match load () with
  | Ok r -> check "restore after gc" (r.txid = 9L && Store.M.find "value" r.kv = "9")
  | Error e -> check ("restore after gc: " ^ e) false);
  check "upto 0 only drops older epochs"
    (Store.garbage [ "e0000000000000001/0000/0000000000000001-0000000000000001.ltx"; "e0000000000000002/0000/0000000000000002-0000000000000002.ltx" ] ~epoch:2 ~upto:0
     = [ "e0000000000000001/0000/0000000000000001-0000000000000001.ltx" ])

(* Alarms: set, not yet due, due, and survives a restore. *)
let () =
  let h = Option.get (Handlers.dispatch "Counter:a") in
  let s = Store.empty () in
  let call ?(query = []) ~now meth path = h s ~now_ms:now ~meth ~path ~query ~body:"" in
  let r = call ~query:[ ("in_ms", "500") ] ~now:1000 "PUT" "/__alarm" in
  check "setting an alarm is a write" (r.wrote && Handlers.alarm_of s = Some 1500);
  let _, ltx = Store.commit s in
  let r = call ~now:1200 "ALARM" "/__alarm/fire" in
  check "early fire is a no-op" ((not r.wrote) && Handlers.alarm_of s = Some 1500);
  let r = call ~now:1500 "ALARM" "/__alarm/fire" in
  check "due fire clears it and runs the class handler"
    (r.wrote && Handlers.alarm_of s = None && Store.M.find "value" s.kv = "1");
  let r = call ~now:1600 "ALARM" "/__alarm/fire" in
  check "second fire is a no-op" (not r.wrote);
  (match Store.restore [ Result.get_ok (Celld_ltx.decode ltx) ] with
  | Ok r -> check "alarm restores with the cell" (Handlers.alarm_of r = Some 1500)
  | Error e -> check e false);
  let kvh = Option.get (Handlers.dispatch "Kv:a") in
  let k = Store.empty () in
  ignore (kvh k ~now_ms:0 ~meth:"PUT" ~path:"/__alarm" ~query:[ ("at_ms", "5") ] ~body:"");
  check "kv hides the reserved key"
    ((kvh k ~now_ms:0 ~meth:"GET" ~path:"/" ~query:[] ~body:"").body = ""
    && (kvh k ~now_ms:0 ~meth:"GET" ~path:"/\000alarm" ~query:[] ~body:"").status = 400)
