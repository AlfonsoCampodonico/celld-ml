module Sql = Celld_sqlite

let check name c = if not c then (Printf.printf "FAIL %s\n" name; exit 1) else Printf.printf "ok   %s\n" name

let image_of db =
  let n, pages = Sql.dirty db in
  let b = Bytes.make (n * Sql.page_size) '\000' in
  List.iter (fun (p, d) -> Bytes.blit_string d 0 b ((p - 1) * Sql.page_size) Sql.page_size) pages;
  Bytes.to_string b

let () =
  let db = Sql.open_image "" in
  check "fresh db is clean" (fst (Sql.dirty db) = 0);
  (match Sql.exec db "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t(b) VALUES ('x'),('y');" with
  | Ok _ -> check "create + insert" true
  | Error e -> check e false);
  let n, pages = Sql.dirty db in
  check "writes dirty pages" (n >= 2 && pages <> []);
  let img = image_of db in
  Sql.clear_dirty db;
  (match Sql.exec db "SELECT a, b FROM t ORDER BY a" with
  | Ok r -> check "select" (r.columns = [ "a"; "b" ] && r.rows = [ [ Some "1"; Some "x" ]; [ Some "2"; Some "y" ] ])
  | Error e -> check e false);
  check "reads dirty nothing" (snd (Sql.dirty db) = []);
  (match Sql.exec db "INSERT INTO nope VALUES (1)" with Error _ -> check "error reported" true | Ok _ -> check "error reported" false);
  (* A database reopened from its image sees the same rows. *)
  let db2 = Sql.open_image img in
  (match Sql.exec db2 "SELECT count(*), max(b) FROM t" with
  | Ok r -> check "reopen from image" (r.rows = [ [ Some "2"; Some "y" ] ])
  | Error e -> check e false);
  check "reopened db is clean" (snd (Sql.dirty db2) = []);
  ignore (Sql.exec db2 "BEGIN; INSERT INTO t(b) VALUES ('z');");
  check "open transaction is visible" (not (Sql.autocommit db2));
  ignore (Sql.exec db2 "ROLLBACK");
  (match Sql.exec db2 "SELECT count(*) FROM t" with
  | Ok r -> check "rollback" (r.rows = [ [ Some "2" ] ] && Sql.autocommit db2)
  | Error e -> check e false);
  Sql.set_now 1_700_000_000_000L;
  (match Sql.exec db2 "SELECT unixepoch()" with
  | Ok r -> check "clock comes from set_now" (r.rows = [ [ Some "1700000000" ] ])
  | Error e -> check e false)
