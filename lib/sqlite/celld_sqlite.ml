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

external bind_null : stmt -> int -> bool = "celld_sql_bind_null"
external bind_int64 : stmt -> int -> int64 -> bool = "celld_sql_bind_int64"
external bind_double : stmt -> int -> float -> bool = "celld_sql_bind_double"
external bind_text : stmt -> int -> string -> bool = "celld_sql_bind_text"
external column_type : stmt -> int -> int = "celld_sql_column_type"
external bind_count : stmt -> int = "celld_sql_bind_count"
external changes : db -> int = "celld_sql_changes"
external last_rowid : db -> int64 = "celld_sql_last_rowid"

let page_size = 4096

type param = Null | Int of int64 | Float of float | Text of string
type value_ = V_null | V_int of string | V_float of string | V_text of string

(* One statement with bound parameters: column names, typed rows, and the
   change count. *)
let exec_bound db sql params =
  match prepare db sql 0 with
  | Error e -> Error e
  | Ok (None, _) -> Ok ([], [], 0, 0L)
  | Ok (Some st, _) ->
      let n = bind_count st in
      if List.length params <> n then (finalize st; Error (Printf.sprintf "statement wants %d parameters, got %d" n (List.length params)))
      else begin
        List.iteri
          (fun i p ->
            ignore
              (match p with
              | Null -> bind_null st (i + 1)
              | Int v -> bind_int64 st (i + 1) v
              | Float f -> bind_double st (i + 1) f
              | Text t -> bind_text st (i + 1) t))
          params;
        let cols = List.init (column_count st) (column_name st) in
        let rec rows acc =
          match step st with
          | 0 ->
              let row =
                List.mapi
                  (fun i _ ->
                    match column_type st i, column_text st i with
                    | _, None | 5, _ -> V_null
                    | 1, Some v -> V_int v
                    | 2, Some v -> V_float v
                    | _, Some v -> V_text v)
                  cols
              in
              rows (row :: acc)
          | 1 -> Ok (List.rev acc)
          | _ -> Error (errmsg db)
        in
        let r = rows [] in
        finalize st;
        Result.map (fun rs -> (cols, rs, changes db, last_rowid db)) r
      end

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
