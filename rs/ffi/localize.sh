#!/bin/sh
# Makes libepochdb_engine.a self-contained for a host that links its own
# copies of blst, secp256k1, zstd, jemalloc or a second Rust runtime (Go's
# avalanchego bls package, Firewood): every object of the archive is merged
# into one relocatable object (ld -r) and every defined symbol but the
# epochdb_* API is made local, so nothing can collide or be picked over the
# host's. Undefined symbols (libc, libm, libdl, pthread, libgcc's unwinder)
# stay. Idempotent; run after every `cargo build` of the archive.
#
#   rs/ffi/localize.sh rs/target/release/libepochdb_engine.a
set -e
A=$1
[ -f "$A" ] || { echo "usage: localize.sh <libepochdb_engine.a>" >&2; exit 2; }
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
ld -r --whole-archive "$A" -o "$T/merged.o"
printf 'epochdb_*\n' > "$T/keep"
objcopy --wildcard --keep-global-symbols="$T/keep" "$T/merged.o" "$T/local.o"
rm -f "$T/out.a"
ar rcs "$T/out.a" "$T/local.o"
mv "$T/out.a" "$A"
echo "localized $A: $(nm -g --defined-only "$A" | grep -c ' T epochdb_') global epochdb_* symbols, $(nm -g --defined-only "$A" | grep -vc ' epochdb_') other globals"
