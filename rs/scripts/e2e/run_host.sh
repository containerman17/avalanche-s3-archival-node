#!/bin/bash
S=${S:-/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad}
# run 1 of the live e2e: genesis -> tip via live fetch from beam validators
cd ~/.herdr/worktrees/epochdb/rust-e2e
exec go run ./cmd/epochdb-host --chain 2tmrrBo1Lgt1mzzvPSFt73kkQKFas5d1AP88tv9cicwoFp8BSn --network mainnet \
  --vm rs/target/release/epochdb-rs --data $S/rs/e2e/data --node https://api.avax.network --http 127.0.0.1:19960 --p2p-port 19961
