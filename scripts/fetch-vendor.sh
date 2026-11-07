#!/usr/bin/env bash
# Fetch the C engines that cells run: SQLite and QuickJS. They are not kept
# in git; this downloads pinned releases, checks their hashes, and applies
# the one-line QuickJS change (CELLD_NO_ATOMICS). Idempotent.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

SQLITE_URL=https://sqlite.org/2026/sqlite-amalgamation-3540100.zip
SQLITE_SHA3_256=92281c86e2957f9b9305890f9ac04204caa7b677cc868dc326d942689df28e30   # published on sqlite.org
QUICKJS_URL=https://bellard.org/quickjs/quickjs-2026-06-04.tar.xz
QUICKJS_SHA256=b376e839b322978313d929fd20663b11ba58b75df5a46c126dd19ea2fa70ad2a    # as first fetched

check() { # file algorithm expected
  local got
  got=$(python3 -c "import hashlib,sys; print(hashlib.new(sys.argv[2], open(sys.argv[1],'rb').read()).hexdigest())" "$1" "$2")
  [ "$got" = "$3" ] || { echo "hash mismatch for $1: $got" >&2; exit 1; }
}

if [ ! -f "$root/lib/sqlite/sqlite3.c" ]; then
  curl -fsSL -o "$tmp/sqlite.zip" "$SQLITE_URL"
  check "$tmp/sqlite.zip" sha3_256 "$SQLITE_SHA3_256"
  (cd "$tmp" && unzip -q sqlite.zip)
  cp "$tmp"/sqlite-amalgamation-*/sqlite3.c "$tmp"/sqlite-amalgamation-*/sqlite3.h "$root/lib/sqlite/"
  echo "fetched SQLite 3.54.1"
fi

if [ ! -f "$root/lib/js/quickjs/quickjs.c" ]; then
  curl -fsSL -o "$tmp/quickjs.tar.xz" "$QUICKJS_URL"
  check "$tmp/quickjs.tar.xz" sha256 "$QUICKJS_SHA256"
  (cd "$tmp" && tar xf quickjs.tar.xz)
  src=$(echo "$tmp"/quickjs-*/)
  mkdir -p "$root/lib/js/quickjs"
  for f in quickjs.c quickjs.h quickjs-atom.h quickjs-opcode.h libregexp.c libregexp.h libregexp-opcode.h \
           libunicode.c libunicode.h libunicode-table.h cutils.c cutils.h dtoa.c dtoa.h list.h VERSION LICENSE; do
    cp "$src/$f" "$root/lib/js/quickjs/"
  done
  # Atomics need OS threads, which Solo5 lacks; keep the rest of the
  # configuration (notably the stack check).
  python3 - "$root/lib/js/quickjs/quickjs.c" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
old = "#if !defined(__EMSCRIPTEN__)\n#define CONFIG_ATOMICS"
new = ("/* celld-ml: CELLD_NO_ATOMICS turns them off without the rest of the\n"
       "   Emscripten configuration (Solo5 has no threads). */\n"
       "#if !defined(__EMSCRIPTEN__) && !defined(CELLD_NO_ATOMICS)\n#define CONFIG_ATOMICS")
assert s.count(old) == 1, "QuickJS patch no longer applies"
open(p, "w").write(s.replace(old, new))
PY
  echo "fetched QuickJS 2026-06-04"
fi
