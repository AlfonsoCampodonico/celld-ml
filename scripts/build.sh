#!/usr/bin/env bash
# Build one unikernel for Solo5 hvt. Run inside the `mirage` Lima VM:
#   scripts/build.sh node|bucket
set -euo pipefail
name=${1:?usage: build.sh node|bucket}
root=$(cd "$(dirname "$0")/.." && pwd)
dir="$root/unikernels/$name"
eval "$(opam env --switch=mirage)"
"$root/scripts/fetch-vendor.sh"
cd "$dir"
ln -sfn ../../lib lib

mirage configure -t hvt
if [ ! -d duniverse ]; then
  make depend
  # Lwt picks up its optional runtime_events support, which needs fstat(2):
  # Solo5 has none, so the link fails.
  rm -rf duniverse/lwt/src/runtime_events duniverse/lwt/examples
  # arm64 under nested virtualisation has a counter too coarse for
  # mirage-crypto's back-to-back timer self-test; keep the source, drop the
  # fatal check.
  sed -i 's/^  timer_check ();/  (try timer_check () with Failure _ -> ());/' \
    duniverse/mirage-crypto/rng/entropy_test.ml
fi

# mirage has no way to name local libraries: add them to the generated stanza.
if [ -f LIBS ]; then
  libs=$(tr '\n' ' ' < LIBS)
  sed -i "s/(libraries /(libraries $libs/" dune.build
fi

dune build --profile release --root . ./dist
# Ship the image without debug information (about a third of the size);
# keep the full one next to it for debugging.
chmod u+w "dist/$name.hvt"
cp "dist/$name.hvt" "dist/$name.hvt.debug"
"$(opam var bin)/aarch64-solo5-ocaml-strip" "dist/$name.hvt" 2>/dev/null \
  || "$(opam var bin)/x86_64-solo5-ocaml-strip" "dist/$name.hvt"
ls -l dist/*.hvt*
