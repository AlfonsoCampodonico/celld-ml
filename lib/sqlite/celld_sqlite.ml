(* A SQLite connection over an in-memory database image whose page writes
   are tracked, so a cell can capture each commit as LTX pages. *)

type db
type stmt

external open_image : string -> db = "celld_sql_open"
external set_now : int64 -> unit = "celld_sql_set_now"
external seed : string -> unit = "celld_sql_seed"
external errmsg : db -> string = "celld_sql_errmsg"
external autocommit : db -> bool = "celld_sql_autocommit"
external dirty : db -> int * (int * string) list = "celld_sql_dirty"
external clear_dirty : db -> unit = "celld_sql_clear_dirty"
external prepare : db -> string -> int -> (stmt option * int, string) result = "celld_sql_prepare"
external step : stmt -> int = "celld_sql_step"
external column_count : stmt -> int = "celld_sql_column_count"
external column_name : stmt -> int -> string = "celld_sql_column_name"
external column_text : stmt -> int -> string option = "celld_sql_column_text"
external finalize : stmt -> unit = "celld_sql_finalize"

let page_size = 4096

type result_set = { columns : string list; rows : string option list list }

(* Run every statement in [sql]. Returns the columns and rows of the last
   statement that produced columns. Stops at the first error. *)
let exec db sql =
  let n = String.length sql in
  let rec go off last =
    if off >= n then Ok last
    else
      match prepare db sql off with
      | Error e -> Error e
      | Ok (None, next) -> if next <= off then Ok last else go next last
      | Ok (Some st, next) ->
          let cols = List.init (column_count st) (column_name st) in
          let rec rows acc =
            match step st with
            | 0 -> rows (List.init (List.length cols) (column_text st) :: acc)
            | 1 -> Ok (List.rev acc)
            | _ -> Error (errmsg db)
          in
          let r = rows [] in
          finalize st;
          (match r with
          | Error e -> Error e
          | Ok rs -> go next (if cols = [] then last else { columns = cols; rows = rs }))
  in
  go 0 { columns = []; rows = [] }
