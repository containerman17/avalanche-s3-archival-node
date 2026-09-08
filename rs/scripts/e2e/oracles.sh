#!/bin/bash
# re-run of the beam 1..1M oracles on the fixed plugin: in-process bench (roots one block behind), then the harness (every block through rpcchainvm)
S=${S:-/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad}
W=$HOME/.herdr/worktrees/epochdb/rust-e2e
rm -rf $S/rs/e2e/oracle-bench-data $S/rs/e2e/oracle-harness-data
echo "== in-process bench $(date '+%H:%M:%S')"
$W/rs/target/release/epochdb-rs --dump $S/rs/beam/beam-containers-1-1000000.bin --genesis $S/rs/beam/chain.json --upgrade $S/rs/beam/upgrade.json --data $S/rs/e2e/oracle-bench-data --to 1000000 --workers 12 > $S/rs/e2e/oracle-bench.log 2>&1
echo "bench exit $? $(date '+%H:%M:%S')"; tail -3 $S/rs/e2e/oracle-bench.log | cut -c1-300
echo "== harness $(date '+%H:%M:%S')"
cd $W && go run ./cmd/epochdb-host-bench --dump $S/rs/beam/beam-containers-1-1000000.bin --vm rs/target/release/epochdb-rs --data $S/rs/e2e/oracle-harness-data --http 127.0.0.1:19970 --batch 256 --config '{"state-sync-enabled":false}' --to 1000000 > $S/rs/e2e/oracle-harness.log 2>&1
echo "harness exit $? $(date '+%H:%M:%S')"; grep -E "check|bench exit|FATAL|mismatch" $S/rs/e2e/oracle-harness.log | cut -c1-300
