/* QuickJS includes <fenv.h>. Solo5's libc ships openlibm's version under
   another name; everywhere else this defers to the system header. */
#ifdef __ocaml_solo5__
#include <openlibm_fenv.h>
#else
#include_next <fenv.h>
#endif
