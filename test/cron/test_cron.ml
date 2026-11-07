open Celld_cron

let check name c = if not c then (Printf.printf "FAIL %s\n" name; exit 1) else Printf.printf "ok   %s\n" name
let ms y m d hh mm = ((days_of_civil y m d * 1440) + (hh * 60) + mm) * 60_000
let p s = match parse s with Ok t -> t | Error e -> failwith (s ^ ": " ^ e)
let nx s ~from = next (p s) ~after_ms:from

let () =
  let t0 = ms 2026 11 7 12 30 in
  check "every minute" (nx "* * * * *" ~from:t0 = Some (ms 2026 11 7 12 31));
  check "*/15" (nx "*/15 * * * *" ~from:t0 = Some (ms 2026 11 7 12 45));
  check "daily at 09:00 rolls to tomorrow" (nx "0 9 * * *" ~from:t0 = Some (ms 2026 11 8 9 0));
  (* 2026-11-07 is a Saturday; weekdays 1-7 from Sunday, so 2 = Monday. *)
  check "weekday 7 is Saturday" (weekday (days_of_civil 2026 11 7) = 7);
  check "MON" (nx "0 0 * * MON" ~from:t0 = Some (ms 2026 11 9 0 0) && nx "0 0 * * 2" ~from:t0 = Some (ms 2026 11 9 0 0));
  check "1-5 is Sunday to Thursday" (nx "0 0 * * 1-5" ~from:t0 = Some (ms 2026 11 8 0 0));
  check "month names" (nx "0 0 1 JAN *" ~from:t0 = Some (ms 2027 1 1 0 0));
  check "L: last day of month" (nx "0 0 L * *" ~from:t0 = Some (ms 2026 11 30 0 0));
  check "L-2" (nx "0 0 L-2 * *" ~from:t0 = Some (ms 2026 11 28 0 0));
  check "LW: last weekday (Nov 30 2026 is a Monday)" (nx "0 0 LW * *" ~from:t0 = Some (ms 2026 11 30 0 0));
  check "15W: Nov 15 2026 is a Sunday, so Monday the 16th" (nx "0 0 15W * *" ~from:t0 = Some (ms 2026 11 16 0 0));
  check "1W on a Saturday moves to Monday the 3rd" (nx "0 0 1W * *" ~from:(ms 2027 4 25 0 0) = Some (ms 2027 5 3 0 0));
  check "6L: last Friday" (nx "0 0 * * 6L" ~from:t0 = Some (ms 2026 11 27 0 0));
  check "2#1: first Monday of December" (nx "0 0 * * 2#1" ~from:(ms 2026 11 30 1 0) = Some (ms 2026 12 7 0 0));
  check "union of restricted day fields" (nx "0 0 1 * MON" ~from:t0 = Some (ms 2026 11 9 0 0) && nx "0 0 1 * MON" ~from:(ms 2026 11 30 1 0) = Some (ms 2026 12 1 0 0));
  check "feb 29" (nx "0 0 29 2 *" ~from:t0 = Some (ms 2028 2 29 0 0));
  check "lists and ranges" (nx "5,10-12 8 * * *" ~from:(ms 2026 11 8 8 10) = Some (ms 2026 11 8 8 11));
  check "a/n means a to the end by n" (nx "50/5 * * * *" ~from:t0 = Some (ms 2026 11 7 12 50) && nx "50/5 * * * *" ~from:(ms 2026 11 7 12 56) = Some (ms 2026 11 7 13 50));
  check "never" (nx "0 0 31 2 *" ~from:t0 = None);
  List.iter
    (fun bad -> check ("refuses " ^ bad) (Result.is_error (parse bad)))
    [ "* * * *"; "60 * * * *"; "*/60 * * * *"; "* * * * SAT-SUN"; "1,* * * * *"; "* * * FOO *"; "* * 0 * *"; "* * * * 8"; "* * * * 2#6" ];
  check "missed occurrences collapse to the latest"
    (latest_due (p "*/10 * * * *") ~after_ms:(ms 2026 11 7 10 0) ~at_ms:(ms 2026 11 7 11 5) = Some (ms 2026 11 7 11 0))
