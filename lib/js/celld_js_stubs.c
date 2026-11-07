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
/* wasm3's internal headers and OCaml's both define code_t (a typedef
   only), so wasm3's is renamed while it is included here. */
#define code_t m3_code_t
#include "wasm3.h"
#include "m3_env.h"
#undef code_t

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
  IM3Environment wenv;            /* WebAssembly, created on first use */
  struct winst *wasm[16];
  int nwasm;
  JSValue wasm_exc;               /* an exception thrown by an import, to rethrow */
};

/* One WebAssembly instance: its runtime (which owns the module), the bytes
   the module was parsed from, its imports, and the one ArrayBuffer that
   aliases its memory. The buffer is detached whenever the memory moves or
   resizes, so JS never holds a dangling view. */
struct wimport { struct cell *c; JSValue fn; IM3FuncType type; };
struct winst {
  IM3Runtime rt;
  uint8_t *bytes;
  struct wimport *imports;
  int nimports;
  JSValue buf;
  uint8_t *buf_ptr;
  uint32_t buf_len;
  int started; /* its start function finished: only then may JS reach it */
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

static void wasm_free(struct cell *c) {
  for (int i = 0; i < c->nwasm; i++) {
    struct winst *w = c->wasm[i];
    if (!JS_IsUndefined(w->buf)) { JS_DetachArrayBuffer(c->ctx, w->buf); JS_FreeValue(c->ctx, w->buf); }
    for (int j = 0; j < w->nimports; j++) JS_FreeValue(c->ctx, w->imports[j].fn);
    if (w->rt) m3_FreeRuntime(w->rt);
    free(w->imports);
    free(w->bytes);
    free(w);
  }
  c->nwasm = 0;
  if (!JS_IsUndefined(c->wasm_exc)) { JS_FreeValue(c->ctx, c->wasm_exc); c->wasm_exc = JS_UNDEFINED; }
  if (c->wenv) { m3_FreeEnvironment(c->wenv); c->wenv = NULL; }
}

static void cell_free(struct cell *c) {
  c->db = NULL;
  if (c->ctx) wasm_free(c);
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
/* __celld_crypto(op, ...args) -> ArrayBuffer: AES-GCM and P-256 in OCaml
   (mirage-crypto). The callback answers "O" ^ result or "E" ^ message.
   Every argument is copied before the next is converted. */
static JSValue js_crypto(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv) {
  (void)this_val;
  if (argc < 1 || argc > 5) return JS_ThrowTypeError(ctx, "bad crypto call");
  const value *f = caml_named_value("celld_js_crypto");
  if (!f) return JS_ThrowTypeError(ctx, "no crypto support");
  const char *op_js = JS_ToCString(ctx, argv[0]);
  if (!op_js) return JS_EXCEPTION;
  char op[32];
  size_t ol = strlen(op_js);
  if (ol >= sizeof op) ol = sizeof op - 1;
  memcpy(op, op_js, ol);
  op[ol] = 0;
  JS_FreeCString(ctx, op_js);
  uint8_t *args[4] = { NULL, NULL, NULL, NULL };
  size_t lens[4] = { 0, 0, 0, 0 };
  int n = argc - 1;
  for (int i = 0; i < n; i++) {
    args[i] = bytes_copy(ctx, argv[i + 1], &lens[i]);
    if (!args[i]) { for (int j = 0; j < i; j++) free(args[j]); return JS_EXCEPTION; }
  }
  JSValue out;
  {
    CAMLparam0();
    CAMLlocal4(o, list, cell, r);
    o = caml_copy_string(op);
    list = Val_emptylist;
    for (int i = n - 1; i >= 0; i--) {
      cell = caml_alloc(2, 0);
      Store_field(cell, 0, ocaml_bytes(args[i], lens[i]));
      Store_field(cell, 1, list);
      list = cell;
    }
    r = caml_callback2(*f, o, list);
    size_t rl = caml_string_length(r);
    const char *rs = String_val(r);
    if (rl == 0 || rs[0] != 'O') {
      char msg[256];
      size_t ml = rl > 1 ? rl - 1 : 0;
      if (ml >= sizeof msg) ml = sizeof msg - 1;
      if (ml) memcpy(msg, rs + 1, ml);
      msg[ml] = 0;
      out = JS_ThrowTypeError(ctx, "%s", ml ? msg : "crypto operation failed");
    } else
      out = JS_NewArrayBufferCopy(ctx, (const uint8_t *)rs + 1, rl - 1);
    CAMLdrop;
  }
  for (int i = 0; i < n; i++) free(args[i]);
  return out;
}


/* ---- WebAssembly on wasm3 ----
   Interpreted, so it runs where a JIT cannot. Each call has an instruction
   budget, charged on calls and loop back-edges (m3_Yield; the vendored
   wasm3 is patched to yield on loops), so a runaway module traps instead
   of stalling the node. Memory is capped per instance. */

#define WASM_BUDGET 20000000
#define WASM_MEMORY_LIMIT (64u << 20)
static int wasm_budget = WASM_BUDGET;
static int wasm_depth = 0;

M3Result m3_Yield(void) {
  return --wasm_budget <= 0 ? "[trap] WebAssembly instruction budget exhausted" : m3Err_none;
}

static void no_free(JSRuntime *rt, void *opaque, void *ptr) { (void)rt; (void)opaque; (void)ptr; }

/* Detach the memory view if the memory moved or resized. */
static void wasm_check_memory(struct cell *c, struct winst *w) {
  if (JS_IsUndefined(w->buf)) return;
  uint32_t len = 0;
  uint8_t *p = m3_GetMemory(w->rt, &len, 0);
  if (p != w->buf_ptr || len != w->buf_len) {
    JS_DetachArrayBuffer(c->ctx, w->buf);
    JS_FreeValue(c->ctx, w->buf);
    w->buf = JS_UNDEFINED;
  }
}

static void wasm_check_all(struct cell *c) { for (int i = 0; i < c->nwasm; i++) wasm_check_memory(c, c->wasm[i]); }

static JSValue wasm_value(JSContext *ctx, uint8_t type, void *slot) {
  switch (type) {
  case c_m3Type_i32: return JS_NewInt32(ctx, *(int32_t *)slot);
  case c_m3Type_i64: return JS_NewBigInt64(ctx, *(int64_t *)slot);
  case c_m3Type_f32: return JS_NewFloat64(ctx, *(float *)slot);
  case c_m3Type_f64: return JS_NewFloat64(ctx, *(double *)slot);
  default: return JS_UNDEFINED;
  }
}

/* JS value -> wasm slot. Returns 0 on success. */
static int wasm_slot(JSContext *ctx, uint8_t type, JSValueConst v, void *slot) {
  switch (type) {
  case c_m3Type_i32: { int32_t x; if (JS_ToInt32(ctx, &x, v)) return -1; *(int32_t *)slot = x; return 0; }
  case c_m3Type_i64: { int64_t x; if (JS_ToBigInt64(ctx, &x, v) && JS_ToInt64(ctx, &x, v)) return -1; JS_FreeValue(ctx, JS_GetException(ctx)); *(int64_t *)slot = x; return 0; }
  case c_m3Type_f32: { double x; if (JS_ToFloat64(ctx, &x, v)) return -1; *(float *)slot = (float)x; return 0; }
  case c_m3Type_f64: { double x; if (JS_ToFloat64(ctx, &x, v)) return -1; *(double *)slot = x; return 0; }
  default: return -1;
  }
}

/* An imported function: call the JS function it was linked to. */
static m3ApiRawFunction(wasm_import_trampoline) {
  (void)runtime; (void)_mem;
  struct wimport *imp = (struct wimport *)_ctx->userdata;
  struct cell *c = imp->c;
  JSContext *ctx = c->ctx;
  IM3FuncType ft = imp->type;
  wasm_check_all(c); /* JS is about to run: no stale memory views */
  int n = ft->numArgs > 16 ? 16 : ft->numArgs;
  JSValue argv[16];
  uint64_t *args = _sp + ft->numRets;
  for (int i = 0; i < n; i++) argv[i] = wasm_value(ctx, ft->types[ft->numRets + i], &args[i]);
  JSValue r = JS_Call(ctx, imp->fn, JS_UNDEFINED, n, argv);
  for (int i = 0; i < n; i++) JS_FreeValue(ctx, argv[i]);
  if (JS_IsException(r)) {
    if (!JS_IsUndefined(c->wasm_exc)) JS_FreeValue(ctx, c->wasm_exc);
    c->wasm_exc = JS_GetException(ctx);
    return "[trap] a JavaScript import threw";
  }
  int bad = ft->numRets ? wasm_slot(ctx, ft->types[0], r, _sp) : 0;
  JS_FreeValue(ctx, r);
  if (bad) {
    if (!JS_IsUndefined(c->wasm_exc)) JS_FreeValue(ctx, c->wasm_exc);
    c->wasm_exc = JS_GetException(ctx);
    return "[trap] a JavaScript import returned a value of the wrong type";
  }
  return m3Err_none;
}

static struct cell *cell_of_ctx(JSContext *ctx) {
  struct cell *c = JS_GetContextOpaque(ctx);
  return c && c->db ? c : NULL;
}

static JSValue wasm_bytes_arg(JSContext *ctx, JSValueConst v, uint8_t **out, size_t *len) {
  *out = bytes_copy(ctx, v, len);
  return *out ? JS_UNDEFINED : JS_EXCEPTION;
}

static JSValue js_wasm_validate(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv) {
  (void)this_val;
  struct cell *c = cell_of_ctx(ctx);
  if (!c || argc < 1) return JS_ThrowTypeError(ctx, "WebAssembly is not available here");
  uint8_t *bytes; size_t len;
  if (JS_IsException(wasm_bytes_arg(ctx, argv[0], &bytes, &len))) return JS_EXCEPTION;
  if (!c->wenv) c->wenv = m3_NewEnvironment();
  IM3Module m = NULL;
  M3Result res = c->wenv ? m3_ParseModule(c->wenv, &m, bytes, (uint32_t)len) : "no environment";
  if (!res && m) m3_FreeModule(m);
  free(bytes);
  return JS_NewBool(ctx, res == m3Err_none);
}

/* __celld_wasm_instantiate(bytes, resolve(module, name) -> function | undefined)
   -> { id, exports: [[name, argc]...], memory } */
static JSValue js_wasm_instantiate(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv) {
  (void)this_val;
  struct cell *c = cell_of_ctx(ctx);
  if (!c || argc < 2) return JS_ThrowTypeError(ctx, "WebAssembly is not available here");
  if (c->nwasm >= 16) return JS_ThrowRangeError(ctx, "a cell holds at most 16 WebAssembly instances");
  if (!c->wenv && !(c->wenv = m3_NewEnvironment())) return JS_ThrowTypeError(ctx, "cannot create a WebAssembly environment");
  struct winst *w = calloc(1, sizeof *w);
  if (!w) return JS_ThrowOutOfMemory(ctx);
  w->buf = JS_UNDEFINED;
  size_t len;
  if (JS_IsException(wasm_bytes_arg(ctx, argv[0], &w->bytes, &len))) { free(w); return JS_EXCEPTION; }
  IM3Module m = NULL;
  M3Result res = m3_ParseModule(c->wenv, &m, w->bytes, (uint32_t)len);
  if (res) { free(w->bytes); free(w); return JS_ThrowTypeError(ctx, "invalid module: %s", res); }
  w->rt = m3_NewRuntime(c->wenv, 64 * 1024, NULL);
  if (!w->rt) { m3_FreeModule(m); free(w->bytes); free(w); return JS_ThrowOutOfMemory(ctx); }
  w->rt->memoryLimit = WASM_MEMORY_LIMIT;
  res = m3_LoadModule(w->rt, m);
  if (res) { m3_FreeModule(m); m3_FreeRuntime(w->rt); free(w->bytes); free(w); return JS_ThrowTypeError(ctx, "cannot load module: %s", res); }
  /* imports: only functions, resolved through the JS callback */
  w->imports = calloc(m->numFuncImports ? m->numFuncImports : 1, sizeof(struct wimport));
  for (u32 i = 0; i < m->numFuncImports; i++) {
    M3Function *f = &m->functions[i];
    JSValue a[2] = { JS_NewString(ctx, f->import.moduleUtf8 ? f->import.moduleUtf8 : ""), JS_NewString(ctx, f->import.fieldUtf8 ? f->import.fieldUtf8 : "") };
    JSValue fn = JS_Call(ctx, argv[1], JS_UNDEFINED, 2, a);
    JS_FreeValue(ctx, a[0]); JS_FreeValue(ctx, a[1]);
    if (JS_IsException(fn) || !JS_IsFunction(ctx, fn)) {
      JS_FreeValue(ctx, fn);
      JS_FreeValue(ctx, JS_GetException(ctx));
      char msg[256];
      snprintf(msg, sizeof msg, "import %s.%s is not a function", f->import.moduleUtf8 ? f->import.moduleUtf8 : "?", f->import.fieldUtf8 ? f->import.fieldUtf8 : "?");
      w->nimports = (int)i;
      c->wasm[c->nwasm++] = w; /* freed with the cell */
      return JS_ThrowTypeError(ctx, "%s", msg);
    }
    IM3FuncType ft = f->funcType;
    char sig[40];
    int k = 0;
    static const char code[] = { 'v', 'i', 'I', 'f', 'F' };
    sig[k++] = ft->numRets ? code[ft->types[0] < 5 ? ft->types[0] : 0] : 'v';
    sig[k++] = '(';
    for (u16 j = 0; j < ft->numArgs && k < 36; j++) { u8 t = ft->types[ft->numRets + j]; sig[k++] = code[t < 5 ? t : 0]; }
    sig[k++] = ')';
    sig[k] = 0;
    w->imports[i] = (struct wimport){ c, fn, ft };
    w->nimports = (int)i + 1;
    res = m3_LinkRawFunctionEx(m, f->import.moduleUtf8, f->import.fieldUtf8, sig, wasm_import_trampoline, &w->imports[i]);
    if (res && res != m3Err_functionLookupFailed) { c->wasm[c->nwasm++] = w; return JS_ThrowTypeError(ctx, "cannot link import: %s", res); }
  }
  c->wasm[c->nwasm++] = w;
  int id = c->nwasm - 1;
  wasm_budget = WASM_BUDGET;
  wasm_depth++;
  res = m3_RunStart(m);
  wasm_depth--;
  /* back in JS: the start function may have moved any instance's memory */
  wasm_check_all(c);
  if (res) {
    if (!JS_IsUndefined(c->wasm_exc)) { JSValue e = c->wasm_exc; c->wasm_exc = JS_UNDEFINED; return JS_Throw(ctx, e); }
    return JS_ThrowTypeError(ctx, "start function trapped: %s", res);
  }
  JSValue out = JS_NewObject(ctx);
  JSValue exports = JS_NewArray(ctx);
  uint32_t n = 0;
  for (u32 i = m->numFuncImports; i < m->numFunctions; i++) {
    M3Function *f = &m->functions[i];
    for (u16 j = 0; j < f->numNames; j++) {
      if (!f->names[j]) continue;
      JSValue e = JS_NewArray(ctx);
      JS_SetPropertyUint32(ctx, e, 0, JS_NewString(ctx, f->names[j]));
      JS_SetPropertyUint32(ctx, e, 1, JS_NewInt32(ctx, f->funcType ? f->funcType->numArgs : 0));
      JS_SetPropertyUint32(ctx, exports, n++, e);
    }
  }
  uint32_t mlen = 0;
  uint8_t *mem = m3_GetMemory(w->rt, &mlen, 0);
  w->started = 1;
  JS_SetPropertyStr(ctx, out, "id", JS_NewInt32(ctx, id));
  JS_SetPropertyStr(ctx, out, "exports", exports);
  JS_SetPropertyStr(ctx, out, "memory", JS_NewBool(ctx, mem != NULL));
  return out;
}

static struct winst *wasm_inst(JSContext *ctx, struct cell *c, JSValueConst idv) {
  int32_t id;
  if (!c || JS_ToInt32(ctx, &id, idv) || id < 0 || id >= c->nwasm || !c->wasm[id]->rt || !c->wasm[id]->started) return NULL;
  return c->wasm[id];
}

/* __celld_wasm_call(id, name, args) */
static JSValue js_wasm_call(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv) {
  (void)this_val;
  struct cell *c = cell_of_ctx(ctx);
  struct winst *w = argc >= 3 ? wasm_inst(ctx, c, argv[0]) : NULL;
  if (!w) return JS_ThrowTypeError(ctx, "no such WebAssembly instance");
  const char *name = JS_ToCString(ctx, argv[1]);
  if (!name) return JS_EXCEPTION;
  IM3Function f = NULL;
  M3Result res = m3_FindFunction(&f, w->rt, name);
  JS_FreeCString(ctx, name);
  if (res || !f) return JS_ThrowTypeError(ctx, "no such export");
  IM3FuncType ft = f->funcType;
  uint64_t slots[16] = { 0 }, rets[4] = { 0 };
  const void *aptrs[16], *rptrs[4];
  int n = ft->numArgs > 16 ? 16 : ft->numArgs;
  for (int i = 0; i < n; i++) {
    JSValue v = JS_GetPropertyUint32(ctx, argv[2], (uint32_t)i);
    int bad = wasm_slot(ctx, ft->types[ft->numRets + i], v, &slots[i]);
    JS_FreeValue(ctx, v);
    if (bad) return JS_EXCEPTION;
    aptrs[i] = &slots[i];
  }
  if (wasm_depth == 0) wasm_budget = WASM_BUDGET;
  wasm_depth++;
  res = m3_Call(f, (uint32_t)n, aptrs);
  wasm_depth--;
  wasm_check_all(c); /* back in JS: no stale memory views */
  if (res) {
    if (!JS_IsUndefined(c->wasm_exc)) { JSValue e = c->wasm_exc; c->wasm_exc = JS_UNDEFINED; return JS_Throw(ctx, e); }
    return JS_ThrowTypeError(ctx, "RuntimeError: %s", res);
  }
  int nr = ft->numRets > 4 ? 4 : ft->numRets;
  if (nr == 0) return JS_UNDEFINED;
  for (int i = 0; i < nr; i++) rptrs[i] = &rets[i];
  if (m3_GetResults(f, (uint32_t)nr, rptrs)) return JS_ThrowTypeError(ctx, "cannot read results");
  return wasm_value(ctx, ft->types[0], &rets[0]);
}

static JSValue js_wasm_memory(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv) {
  (void)this_val;
  struct cell *c = cell_of_ctx(ctx);
  struct winst *w = argc >= 1 ? wasm_inst(ctx, c, argv[0]) : NULL;
  if (!w) return JS_ThrowTypeError(ctx, "no such WebAssembly instance");
  wasm_check_memory(c, w);
  if (JS_IsUndefined(w->buf)) {
    uint32_t len = 0;
    uint8_t *p = m3_GetMemory(w->rt, &len, 0);
    if (!p) return JS_UNDEFINED;
    w->buf = JS_NewArrayBuffer(ctx, p, len, no_free, NULL, 0);
    w->buf_ptr = p;
    w->buf_len = len;
  }
  return JS_DupValue(ctx, w->buf);
}

static JSValue js_wasm_grow(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv) {
  (void)this_val;
  struct cell *c = cell_of_ctx(ctx);
  struct winst *w = argc >= 2 ? wasm_inst(ctx, c, argv[0]) : NULL;
  int32_t delta;
  if (!w || JS_ToInt32(ctx, &delta, argv[1]) || delta < 0) return JS_ThrowTypeError(ctx, "bad grow");
  uint32_t len = 0;
  m3_GetMemory(w->rt, &len, 0);
  uint32_t pages = len / 65536;
  M3Result res = ResizeMemory(w->rt, pages + (uint32_t)delta);
  wasm_check_memory(c, w);
  if (res) return JS_ThrowRangeError(ctx, "cannot grow memory: %s", res);
  return JS_NewInt32(ctx, (int32_t)pages);
}

/* Bytes of WebAssembly memory a cell holds, for memory accounting. */
static size_t wasm_memory_bytes(struct cell *c) {
  size_t total = 0;
  for (int i = 0; i < c->nwasm; i++) { uint32_t len = 0; if (c->wasm[i]->rt && m3_GetMemory(c->wasm[i]->rt, &len, 0)) total += len; }
  return total;
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
  c->wasm_exc = JS_UNDEFINED;
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
  JS_SetPropertyStr(c->ctx, g, "__celld_crypto", JS_NewCFunction(c->ctx, js_crypto, "__celld_crypto", 5));
  JS_SetPropertyStr(c->ctx, g, "__celld_wasm_validate", JS_NewCFunction(c->ctx, js_wasm_validate, "__celld_wasm_validate", 1));
  JS_SetPropertyStr(c->ctx, g, "__celld_wasm_instantiate", JS_NewCFunction(c->ctx, js_wasm_instantiate, "__celld_wasm_instantiate", 2));
  JS_SetPropertyStr(c->ctx, g, "__celld_wasm_call", JS_NewCFunction(c->ctx, js_wasm_call, "__celld_wasm_call", 3));
  JS_SetPropertyStr(c->ctx, g, "__celld_wasm_memory", JS_NewCFunction(c->ctx, js_wasm_memory, "__celld_wasm_memory", 1));
  JS_SetPropertyStr(c->ctx, g, "__celld_wasm_grow", JS_NewCFunction(c->ctx, js_wasm_grow, "__celld_wasm_grow", 2));
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
  return Val_long(u.malloc_size + wasm_memory_bytes(c));
}

/* invoke(cell, fn, arg) -> Ok string | Error message: call the global
   function [fn] with one string, run every job it queues, and return the
   string it returned. The event loop (__celld_start, __celld_resume,
   __celld_poll) runs on this. */
CAMLprim value celld_js_invoke(value cv, value fn_name, value arg) {
  CAMLparam3(cv, fn_name, arg);
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
  JSValue fn = JS_GetPropertyStr(ctx, g, String_val(fn_name));
  JSValue a = JS_NewStringLen(ctx, String_val(arg), caml_string_length(arg));
  JSValue rv = JS_Call(ctx, fn, g, 1, &a);
  JS_FreeValue(ctx, a);
  JS_FreeValue(ctx, fn);
  JS_FreeValue(ctx, g);
  c->budget = CALL_BUDGET;
  JSContext *failed = NULL;
  if (JS_IsException(rv)) {
    char *msg = exception_text(ctx);
    drain(c, &failed);
    CAMLreturn(error_of(msg));
  }
  /* a job that throws is an unhandled rejection: it belongs to its call,
     which sees it as that call's error, not to the loop */
  drain(c, &failed);
  if (failed) JS_FreeValue(failed, JS_GetException(failed));
  size_t n;
  const char *s = JS_ToCStringLen(ctx, &n, rv);
  JS_FreeValue(ctx, rv);
  if (!s) CAMLreturn(error_of(exception_text(ctx)));
  r = caml_alloc(1, 0);
  Store_field(r, 0, caml_alloc_initialized_string(n, s));
  JS_FreeCString(ctx, s);
  CAMLreturn(r);
}
