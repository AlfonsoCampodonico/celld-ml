(* Queues, the outbox and cron at the cell level: every hop the node makes
   over the network is a direct handler call here. *)
open Celld_cell

let check name c = if not c then (Printf.printf "FAIL %s\n" name; exit 1) else Printf.printf "ok   %s\n" name
let now = ref 1_800_000_000_000

let call h s ?(meth = "POST") path body =
  Celld_sqlite.set_now (Int64.of_int !now);
  let r : Handlers.reply = h s ~now_ms:!now ~meth ~path ~query:[] ~body in
  if r.wrote then ignore (Store.commit s);
  r

(* JS handlers return steps; nothing here awaits I/O. *)
let js_cell cell script =
  let s = Store.empty ~sql:true () in
  match Celld_js.create ~script ~db:(Option.get s.sql) with
  | Ok rt ->
      let h = Handlers.js rt cell in
      let sync : Handlers.handler = fun s ~now_ms ~meth ~path ~query ~body ->
        match h s ~now_ms ~meth ~path ~query ~body with Handlers.Done r -> r | Handlers.Await _ -> failwith "unexpected await"
      in
      (s, sync)
  | Error e -> failwith e

let contains hay needle =
  let n = String.length needle in
  let rec go i = i + n <= String.length hay && (String.sub hay i n = needle || go (i + 1)) in
  go 0

let () =
  let producer, ph = js_cell "Shop:1" {|function fetch(r, ctx) { ctx.queue("jobs").send({ order: r.path }); return "queued"; }|} in
  let consumer, ch =
    js_cell "Worker:__consumer"
      {|function queue(batch, ctx) {
          for (const m of batch.messages) {
            if (m.body.order === "/bad") throw new Error("cannot process");
            ctx.storage.put("last", m.body.order); m.ack();
          }
        }|}
  in
  let queue = Store.empty ~sql:true () and qh = Option.get (Handlers.dispatch "Queue:jobs") in
  let dlq = Store.empty ~sql:true () and dh = Option.get (Handlers.dispatch "Queue:dead") in
  ignore (call qh queue "/__queue/config" {|{"consumer":"Worker","max_batch_size":1,"max_retries":1,"dead_letter_queue":"dead"}|});
  let deliver_outbox s h ~to_ =
    List.iter
      (fun (q, payloads, ids) ->
        let qs, qhh = to_ q in
        let r = call qhh qs "/__queue/send" payloads in
        if r.status = 200 then ignore (call h s ~meth:"OUTBOX" "/__outbox/ack" ids))
      (Handlers.outbox_pending s)
  in
  let route = function "jobs" -> (queue, qh) | _ -> (dlq, dh) in
  ignore (call ph producer ~meth:"GET" "/ok" "");
  check "send() lands in the producer's outbox" (List.length (Handlers.outbox_pending producer) = 1);
  deliver_outbox producer ph ~to_:route;
  check "delivered outbox is acknowledged" (Handlers.outbox_pending producer = []);
  deliver_outbox producer ph ~to_:route;
  check "queue holds the message" (contains (call qh queue ~meth:"GET" "/" "").body {|"ready":1|});
  let lease = (call qh queue "/__queue/lease" "").body in
  check "lease hands out a batch" (contains lease {|\"order\":\"/ok\"|} && contains lease {|"attempts":1|});
  let res = call ch consumer ~meth:"QUEUE" "/__queue/deliver" lease in
  check "consumer acks" (res.status = 200 && contains res.body {|"ack":[|});
  let batch = Option.get (Handlers.scalar queue (Printf.sprintf "SELECT json_extract(%s, '$.batch')" (Store.sql_quote lease))) in
  ignore (call qh queue "/__queue/settle" (Printf.sprintf {|{"batch":%S,"ack":%s,"retry":[],"failed":false}|} batch (String.sub res.body 7 (String.length res.body - 8))));
  check "settled message is gone" (contains (call qh queue ~meth:"GET" "/" "").body {|"total":0|});
  check "consumer's writes stuck" (Store.meta_get consumer "x" = None && Handlers.scalar consumer "SELECT v FROM __celld_kv WHERE k = 'last'" = Some {|"/ok"|});
  (* a message whose handler throws: retried, then dead-lettered *)
  ignore (call ph producer ~meth:"GET" "/bad" "");
  deliver_outbox producer ph ~to_:route;
  let attempt () =
    let lease = (call qh queue "/__queue/lease" "").body in
    let res = call ch consumer ~meth:"QUEUE" "/__queue/deliver" lease in
    let batch = Option.value (Handlers.scalar queue (Printf.sprintf "SELECT json_extract(%s, '$.batch')" (Store.sql_quote lease))) ~default:"" in
    ignore (call qh queue "/__queue/settle" (Printf.sprintf {|{"batch":%S,"ack":[],"retry":[],"failed":%b}|} batch (res.status <> 200)));
    res.status
  in
  check "throwing consumer fails the batch" (attempt () = 500);
  check "and its writes are rolled back" (Handlers.scalar consumer "SELECT v FROM __celld_kv WHERE k = 'last'" = Some {|"/ok"|});
  ignore (attempt ());
  check "past max_retries it leaves the queue" (contains (call qh queue ~meth:"GET" "/" "").body {|"total":0|});
  deliver_outbox queue qh ~to_:route;
  check "and lands in the dead-letter queue" (contains (call dh dlq ~meth:"GET" "/" "").body {|"total":1|});
  (* cron *)
  let cron, crh =
    js_cell "Report:__cron"
      {|function scheduled(c, ctx) {
          const n = (ctx.storage.get("runs") || 0) + 1; ctx.storage.put("runs", n); ctx.storage.put("at", c.scheduledTime);
          if (ctx.storage.get("fail")) throw new Error("flaky");
        }|}
  in
  let runs () = Handlers.scalar cron "SELECT v FROM __celld_kv WHERE k = 'runs'" in
  ignore (call crh cron ~meth:"CRON" "/__cron/arm" "*/5 * * * *");
  let alarm = Option.get (Handlers.alarm_of cron) in
  check "arming sets the alarm to the next occurrence" (alarm > !now && alarm mod 300_000 = 0);
  now := alarm;
  ignore (call crh cron ~meth:"ALARM" "/__alarm/fire" "");
  check "occurrence runs scheduled()" (runs () = Some "1");
  check "and the alarm moves to the next one" (Handlers.alarm_of cron = Some (alarm + 300_000));
  now := alarm + 3_600_000;
  ignore (call crh cron ~meth:"ALARM" "/__alarm/fire" "");
  check "after downtime one missed occurrence runs" (runs () = Some "2" && Handlers.scalar cron "SELECT v FROM __celld_kv WHERE k = 'at'" = Some (string_of_int (alarm + 3_600_000)));
  Store.meta_set cron "x" "";
  ignore (Celld_sqlite.exec (Option.get cron.sql) "INSERT OR REPLACE INTO __celld_kv VALUES ('fail', 'true')");
  ignore (Store.commit cron);
  now := alarm + 3_900_000;
  ignore (call crh cron ~meth:"ALARM" "/__alarm/fire" "");
  check "a failing occurrence is rolled back and retried 4 s later"
    (runs () = Some "2" && Handlers.alarm_of cron = Some (alarm + 3_904_000))
