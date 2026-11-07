(* Cron expressions in Cloudflare's dialect, as celld reads them: five
   fields (minute, hour, day of month, month, day of week), one-minute
   resolution, UTC. Weekdays run 1-7 from Sunday. Fields take *, a value,
   a-b, lists and /n steps; month and weekday take three-letter names; the
   day of month takes L, L-n, LW, L-nW and nW; the weekday takes nL and n#k.
   When both day fields are restricted they form a union. Descending ranges,
   lists containing *, and steps wider than the field are refused. *)

type dom_item = Day of int | Last of int | Last_weekday of int | Nearest_weekday of int
type dow_item = Dow of int | Last_dow of int | Nth_dow of int * int

type t = {
  text : string;
  minutes : bool array;
  hours : bool array;
  months : bool array;  (** index 1-12 *)
  dom : dom_item list option;  (** None: unrestricted ( * ) *)
  dow : dow_item list option;
}

let ( let* ) = Result.bind

let months = [ "JAN"; "FEB"; "MAR"; "APR"; "MAY"; "JUN"; "JUL"; "AUG"; "SEP"; "OCT"; "NOV"; "DEC" ]
let days = [ "SUN"; "MON"; "TUE"; "WED"; "THU"; "FRI"; "SAT" ]

let index_of x l =
  let rec go i = function [] -> None | y :: r -> if y = x then Some i else go (i + 1) r in
  go 0 l

let value ~names ~lo ~hi s =
  let s = String.uppercase_ascii s in
  let v =
    match int_of_string_opt s with
    | Some n -> Some n
    | None -> ( match names with Some (ns, first) -> Option.map (fun i -> i + first) (index_of s ns) | None -> None)
  in
  match v with Some n when n >= lo && n <= hi -> Ok n | _ -> Error (Printf.sprintf "bad value %S (expected %d-%d)" s lo hi)

(* One plain item: *, */n, a, a-b, a/n, a-b/n -> the values it selects. *)
let plain ~names ~lo ~hi item =
  let range, step =
    match String.split_on_char '/' item with
    | [ r ] -> (r, None)
    | [ r; s ] -> (r, Some s)
    | _ -> (item, Some "bad")
  in
  let* step =
    match step with
    | None -> Ok 1
    | Some s -> (
        match int_of_string_opt s with
        | Some n when n >= 1 && n <= hi - lo -> Ok n
        | _ -> Error (Printf.sprintf "bad step %S" s))
  in
  let* a, b =
    if range = "*" then Ok (lo, hi)
    else
      match String.split_on_char '-' range with
      | [ a ] ->
          let* a = value ~names ~lo ~hi a in
          Ok (a, if step > 1 || String.contains item '/' then hi else a)
      | [ a; b ] ->
          let* a = value ~names ~lo ~hi a in
          let* b = value ~names ~lo ~hi b in
          if b < a then Error (Printf.sprintf "descending range %S" range) else Ok (a, b)
      | _ -> Error (Printf.sprintf "bad range %S" range)
  in
  Ok (List.filter (fun v -> (v - a) mod step = 0) (List.init (b - a + 1) (fun i -> a + i)))

let items field = String.split_on_char ',' field

let check_star field =
  let is = items field in
  if List.length is > 1 && List.exists (fun i -> i = "*" || String.length i > 1 && String.sub i 0 2 = "*/") is then
    Error (Printf.sprintf "a list cannot contain * (%S)" field)
  else Ok is

let set ~names ~lo ~hi ~size field =
  let* is = check_star field in
  let a = Array.make size false in
  let* () =
    List.fold_left
      (fun acc i ->
        let* () = acc in
        let* vs = plain ~names ~lo ~hi i in
        List.iter (fun v -> a.(v) <- true) vs;
        Ok ())
      (Ok ()) is
  in
  Ok a

let suffix_int s pre =
  let n = String.length pre in
  if String.length s > n && String.sub s 0 n = pre then int_of_string_opt (String.sub s n (String.length s - n)) else None

let dom_field field =
  if field = "*" then Ok None
  else
    let* is = check_star field in
    let item i =
      let u = String.uppercase_ascii i in
      if u = "L" then Ok [ Last 0 ]
      else if u = "LW" then Ok [ Last_weekday 0 ]
      else if String.length u > 2 && String.sub u 0 2 = "L-" then
        let body = String.sub u 2 (String.length u - 2) in
        let w = String.length body > 0 && body.[String.length body - 1] = 'W' in
        let body = if w then String.sub body 0 (String.length body - 1) else body in
        match int_of_string_opt body with
        | Some n when n >= 0 && n <= 30 -> Ok [ (if w then Last_weekday n else Last n) ]
        | _ -> Error (Printf.sprintf "bad day %S" i)
      else if String.length u > 1 && u.[String.length u - 1] = 'W' then
        match int_of_string_opt (String.sub u 0 (String.length u - 1)) with
        | Some n when n >= 1 && n <= 31 -> Ok [ Nearest_weekday n ]
        | _ -> Error (Printf.sprintf "bad day %S" i)
      else
        let* vs = plain ~names:None ~lo:1 ~hi:31 i in
        Ok (List.map (fun d -> Day d) vs)
    in
    let* l = List.fold_left (fun acc i -> let* acc = acc in let* x = item i in Ok (acc @ x)) (Ok []) is in
    Ok (Some l)

let dow_field field =
  if field = "*" then Ok None
  else
    let* is = check_star field in
    let names = Some (days, 1) in
    let item i =
      let u = String.uppercase_ascii i in
      match String.index_opt u '#' with
      | Some k ->
          let* d = value ~names ~lo:1 ~hi:7 (String.sub u 0 k) in
          let* n = match int_of_string_opt (String.sub u (k + 1) (String.length u - k - 1)) with Some n when n >= 1 && n <= 5 -> Ok n | _ -> Error (Printf.sprintf "bad weekday %S" i) in
          Ok [ Nth_dow (d, n) ]
      | None when String.length u > 1 && u.[String.length u - 1] = 'L' ->
          let* d = value ~names ~lo:1 ~hi:7 (String.sub u 0 (String.length u - 1)) in
          Ok [ Last_dow d ]
      | None ->
          let* vs = plain ~names ~lo:1 ~hi:7 i in
          Ok (List.map (fun d -> Dow d) vs)
    in
    let* l = List.fold_left (fun acc i -> let* acc = acc in let* x = item i in Ok (acc @ x)) (Ok []) is in
    Ok (Some l)

let parse text =
  match List.filter (fun s -> s <> "") (String.split_on_char ' ' (String.trim text)) with
  | [ mi; h; dom; mo; dow ] ->
      let* minutes = set ~names:None ~lo:0 ~hi:59 ~size:60 mi in
      let* hours = set ~names:None ~lo:0 ~hi:23 ~size:24 h in
      let* months = set ~names:(Some (months, 1)) ~lo:1 ~hi:12 ~size:13 mo in
      let* dom = dom_field dom in
      let* dow = dow_field dow in
      Ok { text = String.trim text; minutes; hours; months; dom; dow }
  | _ -> Error "a cron expression has five fields: minute hour day-of-month month day-of-week"

(* ---- calendar ---- *)

let days_of_civil y m d =
  let y = if m <= 2 then y - 1 else y in
  let era = (if y >= 0 then y else y - 399) / 400 in
  let yoe = y - (era * 400) in
  let mp = (m + 9) mod 12 in
  let doy = ((153 * mp) + 2) / 5 + d - 1 in
  let doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy in
  (era * 146097) + doe - 719468

let civil_of_days z =
  let z = z + 719468 in
  let era = (if z >= 0 then z else z - 146096) / 146097 in
  let doe = z - (era * 146097) in
  let yoe = (doe - (doe / 1460) + (doe / 36524) - (doe / 146096)) / 365 in
  let doy = doe - ((365 * yoe) + (yoe / 4) - (yoe / 100)) in
  let mp = ((5 * doy) + 2) / 153 in
  let d = doy - (((153 * mp) + 2) / 5) + 1 in
  let m = if mp < 10 then mp + 3 else mp - 9 in
  ((if m <= 2 then yoe + (era * 400) + 1 else yoe + (era * 400)), m, d)

let last_day y m = if m = 12 then 31 else days_of_civil y (m + 1) 1 - days_of_civil y m 1

(* 1 = Sunday *)
let weekday days = ((days mod 7) + 7 + 4) mod 7 + 1

let is_weekday y m d = let w = weekday (days_of_civil y m d) in w >= 2 && w <= 6

let nearest_weekday y m n =
  let ld = last_day y m in
  let n = min n ld in
  let w = weekday (days_of_civil y m n) in
  if w = 7 then (if n = 1 then 3 else n - 1) (* Saturday *)
  else if w = 1 then (if n = ld then n - 2 else n + 1) (* Sunday *)
  else n

let day_matches t y m d =
  let ld = last_day y m in
  let w = weekday (days_of_civil y m d) in
  let dom_ok l =
    List.exists
      (function
        | Day n -> n = d
        | Last off -> d = ld - off
        | Last_weekday off ->
            let target = ld - off in
            let rec back x = if x < 1 then false else if is_weekday y m x then x = d else back (x - 1) in
            target >= 1 && back target
        | Nearest_weekday n -> d = nearest_weekday y m n)
      l
  in
  let dow_ok l =
    List.exists (function Dow k -> k = w | Last_dow k -> k = w && d + 7 > ld | Nth_dow (k, n) -> k = w && ((d - 1) / 7) + 1 = n) l
  in
  match t.dom, t.dow with
  | None, None -> true
  | Some l, None -> dom_ok l
  | None, Some l -> dow_ok l
  | Some a, Some b -> dom_ok a || dow_ok b

(* The first occurrence strictly after [after_ms], within ~5 years. *)
let next t ~after_ms =
  let start_min = (after_ms / 60_000) + 1 in
  let start_day = start_min / 1440 in
  let rec day k =
    if k > 366 * 5 then None
    else
      let dn = start_day + k in
      let y, m, d = civil_of_days dn in
      if t.months.(m) && day_matches t y m d then
        let from = if k = 0 then start_min mod 1440 else 0 in
        let rec minute mm = if mm >= 1440 then None else if t.hours.(mm / 60) && t.minutes.(mm mod 60) then Some mm else minute (mm + 1) in
        match minute from with Some mm -> Some ((((dn * 1440) + mm) * 60_000)) | None -> day (k + 1)
      else day (k + 1)
  in
  day 0

(* The latest occurrence at or before [at_ms] and after [after_ms], if any:
   after downtime, celld runs one missed occurrence and skips the rest. *)
let latest_due t ~after_ms ~at_ms =
  let rec go cur last = match next t ~after_ms:cur with Some n when n <= at_ms -> go n (Some n) | _ -> last in
  go after_ms None
