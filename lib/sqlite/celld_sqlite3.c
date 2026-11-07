/* Compiles the vendored amalgamation for both native builds and Solo5.

   Solo5's libc (ocaml-solo5 nolibc) always enables assert(), whatever
   NDEBUG says, and SQLite's assertions call helpers that only exist in
   debug builds. Including <assert.h> first and redefining assert works
   because its include guard makes SQLite's own include a no-op. It also
   lacks strspn, strcspn and strrchr, which SQLite uses; private versions
   stand in for them under macros, so glibc's declarations are untouched. */

#include <string.h>
#include <assert.h>
#undef assert
#define assert(x) ((void)0)

static size_t celld_strspn(const char *s, const char *accept) {
  size_t n = 0;
  while (s[n] && strchr(accept, s[n])) n++;
  return n;
}

static size_t celld_strcspn(const char *s, const char *reject) {
  size_t n = 0;
  while (s[n] && !strchr(reject, s[n])) n++;
  return n;
}

static char *celld_strrchr(const char *s, int c) {
  const char *last = 0;
  do {
    if (*s == (char)c) last = s;
  } while (*s++);
  return (char *)last;
}

#undef strspn
#undef strcspn
#undef strrchr
#define strspn celld_strspn
#define strcspn celld_strcspn
#define strrchr celld_strrchr

#include "sqlite3.c"
