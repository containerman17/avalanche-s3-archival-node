#!/bin/sh
# Builds and runs the smoke test against a Step dump.
#   rs/ffi/smoke/run.sh <dump> <chain.json> <upgrade.json> <workdir> [n=1000] [valgrind]
set -e
DUMP=$1; CHAIN=$2; UPGRADE=$3; WORK=$4; N=${5:-1000}
RS=$(cd "$(dirname "$0")/../.." && pwd)
mkdir -p "$WORK"
python3 -c "import json,base64,sys; print(base64.b64decode(json.load(open(sys.argv[1]))['genesisData']).decode())" "$CHAIN" > "$WORK/genesis.json"
ALLOC=$(python3 -c "import json,sys; print(list(json.load(open(sys.argv[1]))['alloc'].keys())[0].removeprefix('0x'))" "$WORK/genesis.json")
"$RS/target/release/vbench" --export-inner "$WORK/inner.bin" --dump "$DUMP" --to $((N + 1))
cc -O1 -g -o "$WORK/smoke" "$RS/ffi/smoke/smoke.c" "$RS/target/release/libepochdb_engine.a" -lpthread -ldl -lm
rm -rf "$WORK/data"
if [ "$6" = "valgrind" ]; then
  valgrind --leak-check=full --errors-for-leak-kinds=definite --error-exitcode=9 "$WORK/smoke" "$WORK/data" "$WORK/genesis.json" "$UPGRADE" "$WORK/inner.bin" "$N" "$ALLOC"
else
  "$WORK/smoke" "$WORK/data" "$WORK/genesis.json" "$UPGRADE" "$WORK/inner.bin" "$N" "$ALLOC"
fi
