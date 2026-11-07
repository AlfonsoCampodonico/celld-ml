/* Included first by each wrapper that compiles a QuickJS source file.

   - assert(): Solo5's libc enables it regardless of NDEBUG; turn it off as
     a release build would (see lib/sqlite/celld_sqlite3.c).
   - On Solo5 (__ocaml_solo5__) the compiler also claims __linux__, which
     would pull in <malloc.h>, and there are no threads: Atomics are off
     (CELLD_NO_ATOMICS, a one-line change to the vendored quickjs.c). The
     stack-depth check stays on. */

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#undef assert
#define assert(x) ((void)0)

#ifdef __ocaml_solo5__
#undef __linux__
#undef __gnu_linux__
#define CELLD_NO_ATOMICS 1
/* Only feeds memory accounting; dlmalloc in Solo5's libc does not export it. */
static inline size_t celld_malloc_usable_size(void *p) { (void)p; return 0; }
#define malloc_usable_size celld_malloc_usable_size
/* Missing from Solo5's libc. */
static inline int celld_abs(int x) { return x < 0 ? -x : x; }
#define abs celld_abs
#define alloca __builtin_alloca
static inline char *celld_strrchr(const char *s, int c) {
  const char *last = 0;
  do {
    if (*s == (char)c) last = s;
  } while (*s++);
  return (char *)last;
}
#define strrchr celld_strrchr
/* Date's timezone offset: the unikernel runs in UTC. */
#include <time.h>
struct tm {
  int tm_sec, tm_min, tm_hour, tm_mday, tm_mon, tm_year, tm_wday, tm_yday, tm_isdst;
  long tm_gmtoff;
  const char *tm_zone;
};
static inline struct tm *celld_localtime_r(const time_t *t, struct tm *tm) {
  (void)t;
  memset(tm, 0, sizeof *tm);
  return tm;
}
#define localtime_r celld_localtime_r
#endif
