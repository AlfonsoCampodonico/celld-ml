/* JS cells on QuickJS: one runtime and one context per resident cell, so
   cells share no heap, no memory limit and no job queue.

   A context evaluates a prelude (Request, Response, ctx.storage) and then
   the class script. Calls go through the prelude's __celld_dispatch, which
   may return a promise; pending jobs run until it settles, since a cell has
   no event loop beyond its request. Storage is the cell's own SQLite
   connection, reached through celld_sql_handle. Every call has an
   instruction budget, so a runaway script fails instead of stalling the
   node's actor. */

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "quickjs.h"
#include "sqlite3.h"

#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/callback.h>

extern sqlite3 *celld_sql_handle(value conn);
extern void celld_sql_reset_budget(void);

#define MEMORY_LIMIT ((size_t)32 << 20) /* per cell */
#define STACK_LIMIT ((size_t)256 << 10)
#define CALL_BUDGET 20000 /* interrupt checks per call */
#define MAX_JOBS 10000    /* promise jobs per call */

struct cell {
  JSRuntime *rt;
  JSContext *ctx;
  sqlite3 *db;  /* NULL once disposed */
  int budget;
  int poisoned; /* jobs were left queued: the context must be rebuilt */
  JSValue pending; /* the dispatch promise of a suspended call, or undefined */
};

/* Allocator with a size header, so the memory limit counts real bytes on
   every libc (Solo5's has no malloc_usable_size). The header keeps
   16-byte alignment. */
#define HDR 16

/* Every check is written so it cannot overflow: [size] comes from scripts. */
static int cm_fits(JSMallocState *s, size_t extra) {
  return extra <= s->malloc_limit && s->malloc_size <= s->malloc_limit - extra;
}

static void *cm_malloc(JSMallocState *s, size_t size) {
  if (size > SIZE_MAX - HDR || !cm_fits(s, size + HDR)) return NULL;
  unsigned char *p = malloc(size + HDR);
  if (!p) return NULL;
  *(size_t *)p = size;
  s->malloc_count++;
  s->malloc_size += size + HDR;
  return p + HDR;
}

static size_t cm_usable(const void *ptr) {
  return ptr ? *(const size_t *)((const unsigned char *)ptr - HDR) : 0;
}

static void cm_free(JSMallocState *s, void *ptr) {
  if (!ptr) return;
  s->malloc_count--;
  s->malloc_size -= cm_usable(ptr) + HDR;
  free((unsigned char *)ptr - HDR);
}

static void *cm_realloc(JSMallocState *s, void *ptr, size_t size) {
  if (!ptr) return size ? cm_malloc(s, size) : NULL;
  if (size == 0) { cm_free(s, ptr); return NULL; }
  size_t old = cm_usable(ptr);
  if (size > SIZE_MAX - HDR || (size > old && !cm_fits(s, size - old))) return NULL;
  unsigned char *p = realloc((unsigned char *)ptr - HDR, size + HDR);
  if (!p) return NULL;
  *(size_t *)p = size;
  s->malloc_size = s->malloc_size - old + size;
  return p + HDR;
}

static const JSMallocFunctions counted_malloc = { cm_malloc, cm_free, cm_realloc, cm_usable };

static int on_interrupt(JSRuntime *r, void *opaque) {
  (void)r;
  struct cell *c = opaque;
  return --c->budget <= 0;
}

/* Every entry into JS: a fresh budget, and the stack top measured from
   here, since calls arrive at different depths of the OCaml stack. */
static void enter(struct cell *c) {
  c->budget = CALL_BUDGET;
  JS_UpdateStackTop(c->rt);
}

/* Run queued jobs, at most MAX_JOBS and only while budget remains: a chain
   of tiny jobs may never reach an interrupt check. Jobs left behind would
   run during the next call, so the cell is poisoned and its context rebuilt
   before it serves again. */
static int drain(struct cell *c, JSContext **failed) {
  JSContext *jctx;
  int rc, ok = 1, n = 0;
  while (n++ < MAX_JOBS && c->budget > 0 && (rc = JS_ExecutePendingJob(c->rt, &jctx)) != 0)
    if (rc < 0 && ok) { ok = 0; *failed = jctx; }
  if (JS_IsJobPending(c->rt)) c->poisoned = 1;
  return ok;
}

#define Cell_val(v) (*((struct cell **)Data_custom_val(v)))

static void cell_free(struct cell *c) {
  c->db = NULL;
  if (c->ctx && !JS_IsUndefined(c->pending)) JS_FreeValue(c->ctx, c->pending);
  c->pending = JS_UNDEFINED;
  if (c->ctx) {
    JS_SetContextOpaque(c->ctx, NULL);
    JS_FreeContext(c->ctx);
  }
  if (c->rt) JS_FreeRuntime(c->rt);
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
  if (!c || !c->db) return JS_ThrowTypeError(ctx, "storage is no longer available");
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

/* ---- natives backed by OCaml (registered with Callback.register) ---- */

/* A private copy of the bytes of an ArrayBuffer, a typed array or
   DataView, or a string (UTF-8). Converting a value can run user JS (a
   toString), which could detach or resize another argument's buffer, so
   every argument is copied before the next one is looked at. Returns NULL
   on an exception or out of memory; free with free(). */
static uint8_t *bytes_copy(JSContext *ctx, JSValueConst v, size_t *len) {
  size_t n;
  uint8_t *p = JS_GetArrayBuffer(ctx, &n, v);
  if (!p) {
    JS_FreeValue(ctx, JS_GetException(ctx));
    size_t off, blen, bpe;
    JSValue ab = JS_GetTypedArrayBuffer(ctx, v, &off, &blen, &bpe);
    if (!JS_IsException(ab)) {
      uint8_t *base = JS_GetArrayBuffer(ctx, &n, ab);
      JS_FreeValue(ctx, ab);
      if (base && off + blen <= n) { p = base + off; n = blen; }
    } else
      JS_FreeValue(ctx, JS_GetException(ctx));
  }
  if (p) {
    uint8_t *c = malloc(n ? n : 1);
    if (c) memcpy(c, p, n);
    *len = n;
    return c;
  }
  const char *str = JS_ToCStringLen(ctx, &n, v);
  if (!str) return NULL;
  uint8_t *c = malloc(n ? n : 1);
  if (c) memcpy(c, str, n);
  JS_FreeCString(ctx, str);
  *len = n;
  return c;
}

static value ocaml_bytes(const uint8_t *p, size_t n) { return caml_alloc_initialized_string(n, (const char *)p); }

static JSValue js_log(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv) {
  (void)this_val;
  const value *f = caml_named_value("celld_js_log");
  if (!f || argc < 2) return JS_UNDEFINED;
  size_t n;
  const char *s = JS_ToCStringLen(ctx, &n, argv[1]);
  const char *lvl = JS_ToCString(ctx, argv[0]);
  if (s && lvl) {
    CAMLparam0();
    CAMLlocal2(a, b);
    a = caml_copy_string(lvl);
    b = ocaml_bytes((const uint8_t *)s, n);
    caml_callback2(*f, a, b);
    CAMLdrop;
  }
  if (s) JS_FreeCString(ctx, s);
  if (lvl) JS_FreeCString(ctx, lvl);
  return JS_UNDEFINED;
}

/* __celld_random(n) -> ArrayBuffer of n bytes from the node's CSPRNG. */
static JSValue js_random(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv) {
  (void)this_val;
  int64_t n = 0;
  if (argc < 1 || JS_ToInt64(ctx, &n, argv[0]) || n < 0 || n > 65536) return JS_ThrowRangeError(ctx, "at most 65536 random bytes");
  const value *f = caml_named_value("celld_js_random");
  if (!f) return JS_ThrowTypeError(ctx, "no random source");
  CAMLparam0();
  CAMLlocal1(r);
  r = caml_callback(*f, Val_long(n));
  JSValue out = JS_NewArrayBufferCopy(ctx, (const uint8_t *)String_val(r), caml_string_length(r));
  CAMLdrop;
  return out;
}

/* __celld_digest(alg, data) and __celld_hmac(alg, key, data) -> ArrayBuffer.
   An empty result means an unknown algorithm. */
static JSValue js_hash(JSContext *ctx, int argc, JSValueConst *argv, int hmac) {
  if (argc < (hmac ? 3 : 2)) return JS_ThrowTypeError(ctx, "missing arguments");
  const value *f = caml_named_value(hmac ? "celld_js_hmac" : "celld_js_digest");
  if (!f) return JS_ThrowTypeError(ctx, "no digest support");
  /* Each argument is copied out before the next conversion can run JS. */
  const char *alg_js = JS_ToCString(ctx, argv[0]);
  if (!alg_js) return JS_EXCEPTION;
  char alg[16];
  size_t al = strlen(alg_js);
  if (al >= sizeof alg) al = sizeof alg - 1;
  memcpy(alg, alg_js, al);
  alg[al] = 0;
  JS_FreeCString(ctx, alg_js);
  size_t kl = 0, dl = 0;
  uint8_t *k = NULL;
  if (hmac && !(k = bytes_copy(ctx, argv[1], &kl))) return JS_EXCEPTION;
  uint8_t *d = bytes_copy(ctx, argv[hmac ? 2 : 1], &dl);
  if (!d) { free(k); return JS_EXCEPTION; }
  JSValue out;
  {
    CAMLparam0();
    CAMLlocal4(a, kb, db, r);
    a = caml_copy_string(alg);
    db = ocaml_bytes(d, dl);
    if (hmac) { kb = ocaml_bytes(k, kl); r = caml_callback3(*f, a, kb, db); }
    else r = caml_callback2(*f, a, db);
    if (caml_string_length(r) == 0) out = JS_ThrowTypeError(ctx, "unsupported algorithm %s", alg);
    else out = JS_NewArrayBufferCopy(ctx, (const uint8_t *)String_val(r), caml_string_length(r));
    CAMLdrop;
  }
  free(k);
  free(d);
  return out;
}
static JSValue js_digest(JSContext *ctx, JSValueConst t, int argc, JSValueConst *argv) { (void)t; return js_hash(ctx, argc, argv, 0); }
static JSValue js_hmac(JSContext *ctx, JSValueConst t, int argc, JSValueConst *argv) { (void)t; return js_hash(ctx, argc, argv, 1); }

static int eval(struct cell *c, value src, const char *name) {
  JSContext *ctx = c->ctx;
  enter(c);
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
  c->pending = JS_UNDEFINED;
  c->db = celld_sql_handle(conn);
  c->rt = JS_NewRuntime2(&counted_malloc, NULL);
  if (!c->rt) { free(c); CAMLreturn(error_of(NULL)); }
  JS_SetMemoryLimit(c->rt, MEMORY_LIMIT);
  JS_SetMaxStackSize(c->rt, STACK_LIMIT);
  JS_SetInterruptHandler(c->rt, on_interrupt, c);
  enter(c);
  c->ctx = JS_NewContext(c->rt);
  if (!c->ctx) { cell_free(c); CAMLreturn(error_of(NULL)); }
  JS_SetContextOpaque(c->ctx, c);
  JSValue g = JS_GetGlobalObject(c->ctx);
  JS_SetPropertyStr(c->ctx, g, "__celld_sql_exec", JS_NewCFunction(c->ctx, js_sql_exec, "__celld_sql_exec", 2));
  JS_SetPropertyStr(c->ctx, g, "__celld_log", JS_NewCFunction(c->ctx, js_log, "__celld_log", 2));
  JS_SetPropertyStr(c->ctx, g, "__celld_random", JS_NewCFunction(c->ctx, js_random, "__celld_random", 1));
  JS_SetPropertyStr(c->ctx, g, "__celld_digest", JS_NewCFunction(c->ctx, js_digest, "__celld_digest", 2));
  JS_SetPropertyStr(c->ctx, g, "__celld_hmac", JS_NewCFunction(c->ctx, js_hmac, "__celld_hmac", 3));
  JS_FreeValue(c->ctx, g);
  if (!eval(c, prelude, "prelude.js") || !eval(c, script, "class.js")) {
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

static value settle(struct cell *c, JSValue p);

/* call(cell, name, request_json) -> Ok (Done string | Suspended ops) | Error message */
CAMLprim value celld_js_call(value cv, value name, value req) {
  CAMLparam3(cv, name, req);
  CAMLlocal1(r);
  struct cell *c = Cell_val(cv);
  if (!c) caml_failwith("js: cell disposed");
  if (c->poisoned) {
    static const char msg[] = "poisoned: earlier promise jobs never finished";
    char *m = malloc(sizeof msg);
    if (m) memcpy(m, msg, sizeof msg);
    CAMLreturn(error_of(m));
  }
  JSContext *ctx = c->ctx;
  enter(c);
  JSValue g = JS_GetGlobalObject(ctx);
  JSValue fn = JS_GetPropertyStr(ctx, g, "__celld_dispatch");
  JSValue args[2] = { JS_NewString(ctx, String_val(name)),
                      JS_NewStringLen(ctx, String_val(req), caml_string_length(req)) };
  JSValue p = JS_Call(ctx, fn, g, 2, args);
  JS_FreeValue(ctx, args[0]);
  JS_FreeValue(ctx, args[1]);
  JS_FreeValue(ctx, fn);
  JS_FreeValue(ctx, g);
  JSContext *failed = NULL;
  /* The drain gets its own budget: an interrupted call still has to settle
     its promise through jobs. Total work stays bounded by two budgets plus
     MAX_JOBS short jobs. */
  c->budget = CALL_BUDGET;
  if (JS_IsException(p)) {
    char *msg = exception_text(ctx);
    drain(c, &failed);
    CAMLreturn(error_of(msg));
  }
  if (!drain(c, &failed)) {
    JS_FreeValue(ctx, p);
    CAMLreturn(error_of(exception_text(failed)));
  }
  if (c->poisoned) {
    JS_FreeValue(ctx, p);
    static const char msg[] = "too many promise jobs in one call";
    char *m = malloc(sizeof msg);
    if (m) memcpy(m, msg, sizeof msg);
    CAMLreturn(error_of(m));
  }
  CAMLreturn(settle(c, p));
}

/* The result of a call or resume: Ok (Done s) | Ok (Suspended ops) | Error m. */
static value outcome(int tag, const char *s, size_t n) {
  CAMLparam0();
  CAMLlocal2(o, r);
  o = caml_alloc(1, tag);
  Store_field(o, 0, caml_alloc_initialized_string(n, s));
  r = caml_alloc(1, 0);
  Store_field(r, 0, o);
  CAMLreturn(r);
}

/* A drained dispatch promise [p] (owned): settled, or still waiting on
   operations the node must perform (then kept for celld_js_resume). */
static value settle(struct cell *c, JSValue p) {
  JSContext *ctx = c->ctx;
  JSValue out;
  switch (JS_PromiseState(ctx, p)) {
  case JS_PROMISE_FULFILLED: out = JS_PromiseResult(ctx, p); break;
  case JS_PROMISE_REJECTED: {
    JS_Throw(ctx, JS_PromiseResult(ctx, p));
    JS_FreeValue(ctx, p);
    return error_of(exception_text(ctx));
  }
  case JS_PROMISE_PENDING: {
    JSValue g = JS_GetGlobalObject(ctx);
    JSValue take = JS_GetPropertyStr(ctx, g, "__celld_take_ops");
    JSValue ops = JS_Call(ctx, take, g, 0, NULL);
    JS_FreeValue(ctx, take);
    JS_FreeValue(ctx, g);
    size_t n = 0;
    const char *s = JS_IsException(ops) ? NULL : JS_ToCStringLen(ctx, &n, ops);
    JS_FreeValue(ctx, ops);
    if (s && n > 0) {
      c->pending = p;
      value r = outcome(1, s, n);
      JS_FreeCString(ctx, s);
      return r;
    }
    if (s) JS_FreeCString(ctx, s);
    JS_FreeValue(ctx, p);
    static const char pending[] = "handler awaited something that never settles";
    char *m = malloc(sizeof pending);
    if (m) memcpy(m, pending, sizeof pending);
    return error_of(m);
  }
  default: out = JS_DupValue(ctx, p); /* not a promise */
  }
  JS_FreeValue(ctx, p);
  size_t n;
  const char *s = JS_ToCStringLen(ctx, &n, out);
  JS_FreeValue(ctx, out);
  if (!s) return error_of(exception_text(ctx));
  value r = outcome(0, s, n);
  JS_FreeCString(ctx, s);
  return r;
}

/* resume(cell, results_json): hand a suspended call the results of its
   operations, then run it to its next settle point. */
CAMLprim value celld_js_resume(value cv, value results) {
  CAMLparam2(cv, results);
  struct cell *c = Cell_val(cv);
  if (!c) caml_failwith("js: cell disposed");
  if (JS_IsUndefined(c->pending)) caml_failwith("js: no suspended call");
  JSContext *ctx = c->ctx;
  JSValue p = c->pending;
  c->pending = JS_UNDEFINED;
  enter(c);
  JSValue g = JS_GetGlobalObject(ctx);
  JSValue fn = JS_GetPropertyStr(ctx, g, "__celld_resume");
  JSValue arg = JS_NewStringLen(ctx, String_val(results), caml_string_length(results));
  JSValue rv = JS_Call(ctx, fn, g, 1, &arg);
  JS_FreeValue(ctx, arg);
  JS_FreeValue(ctx, fn);
  JS_FreeValue(ctx, g);
  JS_FreeValue(ctx, rv);
  c->budget = CALL_BUDGET;
  JSContext *failed = NULL;
  if (!drain(c, &failed)) {
    JS_FreeValue(ctx, p);
    CAMLreturn(error_of(exception_text(failed)));
  }
  if (c->poisoned) {
    JS_FreeValue(ctx, p);
    static const char msg[] = "too many promise jobs in one call";
    char *m = malloc(sizeof msg);
    if (m) memcpy(m, msg, sizeof msg);
    CAMLreturn(error_of(m));
  }
  CAMLreturn(settle(c, p));
}

CAMLprim value celld_js_poisoned(value cv) {
  struct cell *c = Cell_val(cv);
  return Val_bool(c == NULL || c->poisoned);
}

CAMLprim value celld_js_dispose(value cv) {
  struct cell *c = Cell_val(cv);
  if (c) cell_free(c);
  Cell_val(cv) = NULL;
  return Val_unit;
}

/* Bytes this cell's runtime has allocated, for memory-pressure shedding. */
CAMLprim value celld_js_memory(value cv) {
  struct cell *c = Cell_val(cv);
  if (!c || !c->rt) return Val_long(0);
  JSMemoryUsage u;
  JS_ComputeMemoryUsage(c->rt, &u);
  return Val_long(u.malloc_size);
}
