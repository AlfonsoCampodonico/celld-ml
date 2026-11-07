/* Included first by each wrapper that compiles a wasm3 source file: the
   same assert() treatment as QuickJS and SQLite (Solo5's libc ignores
   NDEBUG), and nothing else. */
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#undef assert
#define assert(x) ((void)0)

#ifdef __ocaml_solo5__
/* Missing from Solo5's <stdlib.h>; wasm3 uses them to parse numbers given
   as strings (m3_CallArgv), which celld-ml does not call. */
static inline unsigned long long celld_strtoull(const char *s, char **end, int base) {
  unsigned long long v = 0;
  while (*s == ' ' || *s == '\t') s++;
  if ((base == 0 || base == 16) && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) { s += 2; base = 16; }
  if (base == 0) base = 10;
  for (;; s++) {
    int d = (*s >= '0' && *s <= '9') ? *s - '0' : (*s >= 'a' && *s <= 'z') ? *s - 'a' + 10 : (*s >= 'A' && *s <= 'Z') ? *s - 'A' + 10 : 99;
    if (d >= base) break;
    v = v * (unsigned)base + (unsigned)d;
  }
  if (end) *end = (char *)s;
  return v;
}
#define strtoull celld_strtoull
#define strtoul(s, e, b) ((unsigned long)celld_strtoull((s), (e), (b)))
#endif
