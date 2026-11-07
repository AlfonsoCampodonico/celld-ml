/* JS cells on QuickJS: one runtime per node, one context per resident cell.

   A context evaluates a prelude (Request, Response, ctx.storage) and then
   the class script. Calls go through the prelude's __celld_dispatch, which
   may return a promise; pending jobs run until it settles, since a cell has
   no event loop beyond its request. Storage is the cell's own SQLite
   connection, reached through celld_sql_handle. Every call has an
   instruction budget, so a runaway script fails instead of stalling the
   node's actor. */

#include <string.h>
#include <stdlib.h>
#include "quickjs.h"
#include "sqlite3.h"

#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>

extern sqlite3 *celld_sql_handle(value conn);
extern void celld_sql_reset_budget(void);

#define MEMORY_LIMIT ((size_t)64 << 20)
#define STACK_LIMIT ((size_t)512 << 10)
#define CALL_BUDGET 20000 /* interrupt checks per call */

static JSRuntime *rt = NULL;
static int budget = CALL_BUDGET;

static int on_interrupt(JSRuntime *r, void *opaque) {
  (void)r; (void)opaque;
  return --budget <= 0;
}

static JSRuntime *runtime(void) {
  if (!rt) {
    rt = JS_NewRuntime();
    if (!rt) caml_failwith("js: cannot create runtime");
    JS_SetMemoryLimit(rt, MEMORY_LIMIT);
    JS_SetMaxStackSize(rt, STACK_LIMIT);
    JS_SetInterruptHandler(rt, on_interrupt, NULL);
  }
  return rt;
}

struct cell {
  JSContext *ctx;
  sqlite3 *db;
};

#define Cell_val(v) (*((struct cell **)Data_custom_val(v)))

static void cell_free(struct cell *c) {
  if (c->ctx) JS_FreeContext(c->ctx);
  free(c);
}

static void cell_finalize(value v) {
  struct cell *c = Cell_val(v);
  if (c) cell_free(c);
  Cell_val(v) = NULL;
}

static struct custom_operations cell_ops = {
  "celld.js.cell", cell_finalize, custom_compare_default, custom_hash_default,
  custom_serialize_default, custom_deserialize_default, custom_compare_ext_default,
  custom_fixed_length_default
};

/* The message of the pending exception, with its stack when there is one. */
static char *exception_text(JSContext *ctx) {
  JSValue e = JS_GetException(ctx);
  const char *msg = JS_ToCString(ctx, e);
  JSValue stack = JS_IsError(ctx, e) ? JS_GetPropertyStr(ctx, e, "stack") : JS_UNDEFINED;
  const char *st = JS_IsUndefined(stack) ? NULL : JS_ToCString(ctx, stack);
  const char *m = msg ? msg : "unknown";
  size_t ml = strlen(m), sl = (st && *st) ? strlen(st) : 0;
  char *out = malloc(ml + (sl ? sl + 1 : 0) + 1);
  if (out) {
    memcpy(out, m, ml);
    if (sl) { out[ml] = '\n'; memcpy(out + ml + 1, st, sl); }
    out[ml + (sl ? sl + 1 : 0)] = 0;
  }
  if (msg) JS_FreeCString(ctx, msg);
  if (st) JS_FreeCString(ctx, st);
  JS_FreeValue(ctx, stack);
  JS_FreeValue(ctx, e);
  return out;
}

static value error_of(char *msg) {
  CAMLparam0();
  CAMLlocal1(r);
  r = caml_alloc(1, 1);
  Store_field(r, 0, caml_copy_string(msg ? msg : "out of memory"));
  free(msg);
  CAMLreturn(r);
}

/* __celld_sql_exec(sql, binds) -> array of row objects. One statement. */
static JSValue js_sql_exec(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv) {
  (void)this_val;
  struct cell *c = JS_GetContextOpaque(ctx);
  if (argc < 1) return JS_ThrowTypeError(ctx, "sql.exec(query, ...bindings)");
  size_t qlen;
  const char *q = JS_ToCStringLen(ctx, &qlen, argv[0]);
  if (!q) return JS_EXCEPTION;
  sqlite3_stmt *st = NULL;
  celld_sql_reset_budget();
  int rc = sqlite3_prepare_v2(c->db, q, (int)qlen, &st, NULL);
  JS_FreeCString(ctx, q);
  if (rc != SQLITE_OK) return JS_ThrowTypeError(ctx, "%s", sqlite3_errmsg(c->db));
  if (!st) return JS_NewArray(ctx);
  if (argc > 1 && JS_IsArray(ctx, argv[1])) {
    JSValue len_v = JS_GetPropertyStr(ctx, argv[1], "length");
    int64_t n = 0;
    JS_ToInt64(ctx, &n, len_v);
    JS_FreeValue(ctx, len_v);
    for (int64_t i = 0; i < n; i++) {
      JSValue b = JS_GetPropertyUint32(ctx, argv[1], (uint32_t)i);
      int idx = (int)i + 1;
      if (JS_IsNull(b) || JS_IsUndefined(b)) sqlite3_bind_null(st, idx);
      else if (JS_IsBool(b)) sqlite3_bind_int(st, idx, JS_ToBool(ctx, b));
      else if (JS_IsNumber(b)) {
        double d;
        JS_ToFloat64(ctx, &d, b);
        if (d == (double)(int64_t)d) sqlite3_bind_int64(st, idx, (int64_t)d);
        else sqlite3_bind_double(st, idx, d);
      } else {
        size_t sl;
        const char *s = JS_ToCStringLen(ctx, &sl, b);
        if (!s) { JS_FreeValue(ctx, b); sqlite3_finalize(st); return JS_EXCEPTION; }
        sqlite3_bind_text(st, idx, s, (int)sl, SQLITE_TRANSIENT);
        JS_FreeCString(ctx, s);
      }
      JS_FreeValue(ctx, b);
    }
  }
  JSValue rows = JS_NewArray(ctx);
  uint32_t r = 0;
  int ncol = sqlite3_column_count(st);
  while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
    JSValue o = JS_NewObject(ctx);
    for (int i = 0; i < ncol; i++) {
      JSValue v;
      switch (sqlite3_column_type(st, i)) {
      case SQLITE_INTEGER: v = JS_NewInt64(ctx, sqlite3_column_int64(st, i)); break;
      case SQLITE_FLOAT: v = JS_NewFloat64(ctx, sqlite3_column_double(st, i)); break;
      case SQLITE_NULL: v = JS_NULL; break;
      default:
        v = JS_NewStringLen(ctx, (const char *)sqlite3_column_text(st, i), (size_t)sqlite3_column_bytes(st, i));
      }
      JS_SetPropertyStr(ctx, o, sqlite3_column_name(st, i), v);
    }
    JS_SetPropertyUint32(ctx, rows, r++, o);
  }
  if (rc != SQLITE_DONE) {
    JSValue err = JS_ThrowTypeError(ctx, "%s", sqlite3_errmsg(c->db));
    sqlite3_finalize(st);
    JS_FreeValue(ctx, rows);
    return err;
  }
  sqlite3_finalize(st);
  return rows;
}

static int eval(JSContext *ctx, value src, const char *name) {
  budget = CALL_BUDGET;
  JSValue v = JS_Eval(ctx, String_val(src), caml_string_length(src), name, JS_EVAL_TYPE_GLOBAL);
  if (JS_IsException(v)) return 0;
  JS_FreeValue(ctx, v);
  return 1;
}

/* create(prelude, script, conn) -> Ok cell | Error message */
CAMLprim value celld_js_create(value prelude, value script, value conn) {
  CAMLparam3(prelude, script, conn);
  CAMLlocal2(r, v);
  struct cell *c = calloc(1, sizeof *c);
  if (!c) caml_raise_out_of_memory();
  c->db = celld_sql_handle(conn);
  c->ctx = JS_NewContext(runtime());
  if (!c->ctx) { free(c); CAMLreturn(error_of(NULL)); }
  JS_SetContextOpaque(c->ctx, c);
  JSValue g = JS_GetGlobalObject(c->ctx);
  JS_SetPropertyStr(c->ctx, g, "__celld_sql_exec", JS_NewCFunction(c->ctx, js_sql_exec, "__celld_sql_exec", 2));
  JS_FreeValue(c->ctx, g);
  if (!eval(c->ctx, prelude, "prelude.js") || !eval(c->ctx, script, "class.js")) {
    char *msg = exception_text(c->ctx);
    cell_free(c);
    CAMLreturn(error_of(msg));
  }
  v = caml_alloc_custom(&cell_ops, sizeof(struct cell *), 0, 1);
  Cell_val(v) = c;
  r = caml_alloc(1, 0);
  Store_field(r, 0, v);
  CAMLreturn(r);
}

/* call(cell, name, request_json) -> Ok string | Error message */
CAMLprim value celld_js_call(value cv, value name, value req) {
  CAMLparam3(cv, name, req);
  CAMLlocal1(r);
  struct cell *c = Cell_val(cv);
  if (!c) caml_failwith("js: cell disposed");
  JSContext *ctx = c->ctx;
  budget = CALL_BUDGET;
  JSValue g = JS_GetGlobalObject(ctx);
  JSValue fn = JS_GetPropertyStr(ctx, g, "__celld_dispatch");
  JSValue args[2] = { JS_NewString(ctx, String_val(name)),
                      JS_NewStringLen(ctx, String_val(req), caml_string_length(req)) };
  JSValue p = JS_Call(ctx, fn, g, 2, args);
  JS_FreeValue(ctx, args[0]);
  JS_FreeValue(ctx, args[1]);
  JS_FreeValue(ctx, fn);
  JS_FreeValue(ctx, g);
  if (JS_IsException(p)) CAMLreturn(error_of(exception_text(ctx)));
  JSContext *jctx;
  int jobs;
  while ((jobs = JS_ExecutePendingJob(rt, &jctx)) > 0) {}
  if (jobs < 0) {
    JS_FreeValue(ctx, p);
    CAMLreturn(error_of(exception_text(jctx)));
  }
  JSValue out;
  switch (JS_PromiseState(ctx, p)) {
  case JS_PROMISE_FULFILLED: out = JS_PromiseResult(ctx, p); break;
  case JS_PROMISE_REJECTED: {
    JS_Throw(ctx, JS_PromiseResult(ctx, p));
    JS_FreeValue(ctx, p);
    CAMLreturn(error_of(exception_text(ctx)));
  }
  case JS_PROMISE_PENDING: {
    JS_FreeValue(ctx, p);
    static const char pending[] = "handler awaited something that never settles";
    char *m = malloc(sizeof pending);
    if (m) memcpy(m, pending, sizeof pending);
    CAMLreturn(error_of(m));
  }
  default: out = JS_DupValue(ctx, p); /* not a promise */
  }
  JS_FreeValue(ctx, p);
  size_t n;
  const char *s = JS_ToCStringLen(ctx, &n, out);
  JS_FreeValue(ctx, out);
  if (!s) CAMLreturn(error_of(exception_text(ctx)));
  r = caml_alloc(1, 0);
  Store_field(r, 0, caml_alloc_initialized_string(n, s));
  JS_FreeCString(ctx, s);
  CAMLreturn(r);
}

CAMLprim value celld_js_dispose(value cv) {
  struct cell *c = Cell_val(cv);
  if (c) cell_free(c);
  Cell_val(cv) = NULL;
  return Val_unit;
}
