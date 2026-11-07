/* SQLite for celld-ml cells: an in-memory VFS with dirty-page tracking, and
   the few calls the OCaml side needs.

   SQLite is built with SQLITE_OS_OTHER, so this file is its whole OS layer:
   no threads, no files, no clock but the one OCaml sets. Each connection's
   main database lives in one growable buffer. Every write marks the pages
   it touches; after a statement the cell asks which pages changed and turns
   them into an LTX file, the way celld captures SQLite's WAL frames. */

#include <string.h>
#include <stdlib.h>
#include "sqlite3.h"

#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>

#define PAGE 4096

/* ---- the database image ---- */

struct img {
  unsigned char *data;
  sqlite3_int64 len, cap;
  unsigned char *dirty; /* one byte per page */
  sqlite3_int64 dirty_cap;
};

static int img_reserve(struct img *m, sqlite3_int64 len) {
  if (len > m->cap) {
    sqlite3_int64 cap = m->cap ? m->cap : 16 * PAGE;
    while (cap < len) cap *= 2;
    unsigned char *d = realloc(m->data, (size_t)cap);
    if (!d) return 0;
    memset(d + m->cap, 0, (size_t)(cap - m->cap));
    m->data = d;
    m->cap = cap;
  }
  sqlite3_int64 pages = (len + PAGE - 1) / PAGE;
  if (pages > m->dirty_cap) {
    sqlite3_int64 cap = m->dirty_cap ? m->dirty_cap : 64;
    while (cap < pages) cap *= 2;
    unsigned char *d = realloc(m->dirty, (size_t)cap);
    if (!d) return 0;
    memset(d + m->dirty_cap, 0, (size_t)(cap - m->dirty_cap));
    m->dirty = d;
    m->dirty_cap = cap;
  }
  return 1;
}

static void img_free(struct img *m) {
  free(m->data);
  free(m->dirty);
  free(m);
}

/* xOpen of the main database adopts this image (single-threaded). */
static struct img *pending_main = NULL;
static sqlite3_int64 now_ms = 0;
static unsigned long long rng = 0x9e3779b97f4a7c15ULL;

/* ---- sqlite3_io_methods ---- */

struct cfile {
  sqlite3_file base;
  struct img *img;
  int owned; /* journals and temp files own their image */
};

static int f_close(sqlite3_file *f) {
  struct cfile *c = (struct cfile *)f;
  if (c->owned) img_free(c->img);
  return SQLITE_OK;
}

static int f_read(sqlite3_file *f, void *buf, int n, sqlite3_int64 off) {
  struct img *m = ((struct cfile *)f)->img;
  if (off >= m->len) {
    memset(buf, 0, (size_t)n);
    return SQLITE_IOERR_SHORT_READ;
  }
  sqlite3_int64 have = m->len - off;
  if (have >= n) {
    memcpy(buf, m->data + off, (size_t)n);
    return SQLITE_OK;
  }
  memcpy(buf, m->data + off, (size_t)have);
  memset((char *)buf + have, 0, (size_t)(n - have));
  return SQLITE_IOERR_SHORT_READ;
}

static int f_write(sqlite3_file *f, const void *buf, int n, sqlite3_int64 off) {
  struct img *m = ((struct cfile *)f)->img;
  if (!img_reserve(m, off + n)) return SQLITE_IOERR_WRITE;
  memcpy(m->data + off, buf, (size_t)n);
  if (off + n > m->len) m->len = off + n;
  for (sqlite3_int64 p = off / PAGE; p <= (off + n - 1) / PAGE; p++) m->dirty[p] = 1;
  return SQLITE_OK;
}

static int f_truncate(sqlite3_file *f, sqlite3_int64 size) {
  struct img *m = ((struct cfile *)f)->img;
  if (size < m->len) {
    memset(m->data + size, 0, (size_t)(m->len - size));
    m->len = size;
  }
  return SQLITE_OK;
}

static int f_sync(sqlite3_file *f, int flags) { (void)f; (void)flags; return SQLITE_OK; }

static int f_size(sqlite3_file *f, sqlite3_int64 *size) {
  *size = ((struct cfile *)f)->img->len;
  return SQLITE_OK;
}

static int f_lock(sqlite3_file *f, int l) { (void)f; (void)l; return SQLITE_OK; }
static int f_check_lock(sqlite3_file *f, int *out) { (void)f; *out = 0; return SQLITE_OK; }
static int f_control(sqlite3_file *f, int op, void *arg) { (void)f; (void)op; (void)arg; return SQLITE_NOTFOUND; }
static int f_sector(sqlite3_file *f) { (void)f; return PAGE; }
static int f_caps(sqlite3_file *f) { (void)f; return SQLITE_IOCAP_SAFE_APPEND | SQLITE_IOCAP_POWERSAFE_OVERWRITE; }

static const sqlite3_io_methods io = {
  1, f_close, f_read, f_write, f_truncate, f_sync, f_size,
  f_lock, f_lock, f_check_lock, f_control, f_sector, f_caps,
  0, 0, 0, 0, 0, 0
};

/* ---- sqlite3_vfs ---- */

static int v_open(sqlite3_vfs *v, sqlite3_filename name, sqlite3_file *f, int flags, int *out) {
  (void)v; (void)name;
  struct cfile *c = (struct cfile *)f;
  memset(c, 0, sizeof *c);
  if ((flags & SQLITE_OPEN_MAIN_DB) && pending_main) {
    c->img = pending_main;
    c->owned = 0;
    pending_main = NULL;
  } else {
    c->img = calloc(1, sizeof(struct img));
    if (!c->img) return SQLITE_NOMEM;
    c->owned = 1;
  }
  c->base.pMethods = &io;
  if (out) *out = flags;
  return SQLITE_OK;
}

static int v_delete(sqlite3_vfs *v, const char *n, int s) { (void)v; (void)n; (void)s; return SQLITE_OK; }
static int v_access(sqlite3_vfs *v, const char *n, int fl, int *out) { (void)v; (void)n; (void)fl; *out = 0; return SQLITE_OK; }

static int v_fullpath(sqlite3_vfs *v, const char *n, int len, char *out) {
  (void)v;
  sqlite3_snprintf(len, out, "%s", n);
  return SQLITE_OK;
}

static int v_random(sqlite3_vfs *v, int n, char *out) {
  (void)v;
  for (int i = 0; i < n; i++) {
    rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
    out[i] = (char)(rng & 0xff);
  }
  return n;
}

static int v_sleep(sqlite3_vfs *v, int us) { (void)v; return us; }

/* Julian day numbers, as SQLite expects. */
static int v_time64(sqlite3_vfs *v, sqlite3_int64 *out) {
  (void)v;
  *out = 210866760000000LL + now_ms;
  return SQLITE_OK;
}

static int v_time(sqlite3_vfs *v, double *out) {
  sqlite3_int64 t;
  v_time64(v, &t);
  *out = (double)t / 86400000.0;
  return SQLITE_OK;
}

static int v_error(sqlite3_vfs *v, int n, char *out) { (void)v; if (n > 0) out[0] = 0; return 0; }

static sqlite3_vfs vfs = {
  2, sizeof(struct cfile), 64, 0, "celld", 0,
  v_open, v_delete, v_access, v_fullpath,
  0, 0, 0, 0,
  v_random, v_sleep, v_time, v_error, v_time64,
  0, 0, 0
};

int sqlite3_os_init(void) { return sqlite3_vfs_register(&vfs, 1); }
int sqlite3_os_end(void) { return SQLITE_OK; }

/* ---- OCaml: connections ---- */

struct conn {
  sqlite3 *db;
  struct img *img;
};

#define Conn_val(v) (*((struct conn **)Data_custom_val(v)))

static void conn_finalize(value v) {
  struct conn *c = Conn_val(v);
  if (c) {
    if (c->db) sqlite3_close_v2(c->db);
    if (c->img) img_free(c->img);
    free(c);
  }
}

static struct custom_operations conn_ops = {
  "celld.sqlite.conn", conn_finalize, custom_compare_default, custom_hash_default,
  custom_serialize_default, custom_deserialize_default, custom_compare_ext_default,
  custom_fixed_length_default
};

static struct conn *conn_of(value v) {
  struct conn *c = Conn_val(v);
  if (!c || !c->db) caml_failwith("sqlite: connection closed");
  return c;
}

/* Open a connection over [image] (empty for a new database). */
CAMLprim value celld_sql_open(value image) {
  CAMLparam1(image);
  CAMLlocal1(r);
  struct conn *c = calloc(1, sizeof *c);
  if (!c) caml_raise_out_of_memory();
  c->img = calloc(1, sizeof(struct img));
  if (!c->img) { free(c); caml_raise_out_of_memory(); }
  sqlite3_int64 len = caml_string_length(image);
  if (len > 0) {
    if (!img_reserve(c->img, len)) { img_free(c->img); free(c); caml_raise_out_of_memory(); }
    memcpy(c->img->data, String_val(image), (size_t)len);
    c->img->len = len;
  }
  pending_main = c->img;
  int rc = sqlite3_open_v2("cell.db", &c->db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "celld");
  pending_main = NULL;
  if (rc == SQLITE_OK)
    rc = sqlite3_exec(c->db,
                      "PRAGMA journal_mode=MEMORY; PRAGMA synchronous=OFF; "
                      "PRAGMA locking_mode=EXCLUSIVE; PRAGMA temp_store=MEMORY;",
                      0, 0, 0);
  if (rc != SQLITE_OK) {
    if (c->db) sqlite3_close_v2(c->db);
    img_free(c->img);
    free(c);
    caml_failwith("sqlite: cannot open database");
  }
  /* Pragmas touch no pages; anything dirty now came from [image]. */
  if (c->img->dirty) memset(c->img->dirty, 0, (size_t)c->img->dirty_cap);
  r = caml_alloc_custom(&conn_ops, sizeof(struct conn *), 0, 1);
  Conn_val(r) = c;
  CAMLreturn(r);
}

CAMLprim value celld_sql_set_now(value ms) {
  now_ms = Int64_val(ms);
  return Val_unit;
}

CAMLprim value celld_sql_errmsg(value v) {
  CAMLparam1(v);
  CAMLreturn(caml_copy_string(sqlite3_errmsg(conn_of(v)->db)));
}

CAMLprim value celld_sql_autocommit(value v) {
  return Val_bool(sqlite3_get_autocommit(conn_of(v)->db));
}

/* Size in pages, and the pages written since the last [clear_dirty], as an
   (int * string) list. */
CAMLprim value celld_sql_dirty(value v) {
  CAMLparam1(v);
  CAMLlocal4(list, cell, pair, page);
  struct img *m = conn_of(v)->img;
  sqlite3_int64 npages = (m->len + PAGE - 1) / PAGE;
  list = Val_emptylist;
  for (sqlite3_int64 p = npages - 1; p >= 0; p--) {
    if (!m->dirty[p]) continue;
    page = caml_alloc_string(PAGE);
    memcpy((char *)Bytes_val(page), m->data + p * PAGE, PAGE);
    pair = caml_alloc_tuple(2);
    Store_field(pair, 0, Val_long(p + 1));
    Store_field(pair, 1, page);
    cell = caml_alloc_tuple(2);
    Store_field(cell, 0, pair);
    Store_field(cell, 1, list);
    list = cell;
  }
  pair = caml_alloc_tuple(2);
  Store_field(pair, 0, Val_long(npages));
  Store_field(pair, 1, list);
  CAMLreturn(pair);
}

CAMLprim value celld_sql_clear_dirty(value v) {
  struct img *m = conn_of(v)->img;
  if (m->dirty) memset(m->dirty, 0, (size_t)m->dirty_cap);
  return Val_unit;
}

/* ---- OCaml: statements ---- */

#define Stmt_val(v) (*((sqlite3_stmt **)Data_custom_val(v)))

static struct custom_operations stmt_ops = {
  "celld.sqlite.stmt", custom_finalize_default, custom_compare_default, custom_hash_default,
  custom_serialize_default, custom_deserialize_default, custom_compare_ext_default,
  custom_fixed_length_default
};

/* Prepare the first statement of [sql] from byte [off]: returns
   Ok (Some stmt | None, next offset) or Error message. */
CAMLprim value celld_sql_prepare(value v, value sql, value off) {
  CAMLparam3(v, sql, off);
  CAMLlocal4(r, tup, opt, s);
  sqlite3 *db = conn_of(v)->db;
  const char *start = String_val(sql) + Long_val(off);
  int len = (int)(caml_string_length(sql) - Long_val(off));
  const char *tail = NULL;
  sqlite3_stmt *st = NULL;
  int rc = sqlite3_prepare_v2(db, start, len, &st, &tail);
  if (rc != SQLITE_OK) {
    r = caml_alloc(1, 1);
    Store_field(r, 0, caml_copy_string(sqlite3_errmsg(db)));
    CAMLreturn(r);
  }
  if (st) {
    s = caml_alloc_custom(&stmt_ops, sizeof(sqlite3_stmt *), 0, 1);
    Stmt_val(s) = st;
    opt = caml_alloc(1, 0);
    Store_field(opt, 0, s);
  } else
    opt = Val_none;
  tup = caml_alloc_tuple(2);
  Store_field(tup, 0, opt);
  Store_field(tup, 1, Val_long(Long_val(off) + (tail ? (long)(tail - start) : len)));
  r = caml_alloc(1, 0);
  Store_field(r, 0, tup);
  CAMLreturn(r);
}

/* 0 = row, 1 = done, 2 = error */
CAMLprim value celld_sql_step(value s) {
  int rc = sqlite3_step(Stmt_val(s));
  return Val_int(rc == SQLITE_ROW ? 0 : rc == SQLITE_DONE ? 1 : 2);
}

CAMLprim value celld_sql_column_count(value s) { return Val_int(sqlite3_column_count(Stmt_val(s))); }

CAMLprim value celld_sql_column_name(value s, value i) {
  CAMLparam2(s, i);
  const char *n = sqlite3_column_name(Stmt_val(s), Int_val(i));
  CAMLreturn(caml_copy_string(n ? n : ""));
}

CAMLprim value celld_sql_column_text(value s, value i) {
  CAMLparam2(s, i);
  CAMLlocal2(r, str);
  sqlite3_stmt *st = Stmt_val(s);
  if (sqlite3_column_type(st, Int_val(i)) == SQLITE_NULL) CAMLreturn(Val_none);
  const unsigned char *t = sqlite3_column_text(st, Int_val(i));
  int n = sqlite3_column_bytes(st, Int_val(i));
  str = caml_alloc_initialized_string(n, (const char *)t);
  r = caml_alloc(1, 0);
  Store_field(r, 0, str);
  CAMLreturn(r);
}

CAMLprim value celld_sql_finalize(value s) {
  if (Stmt_val(s)) sqlite3_finalize(Stmt_val(s));
  Stmt_val(s) = NULL;
  return Val_unit;
}
