#!/bin/bash
# tipfollow.sh LOCAL_RPC REMOTE_RPC SECONDS INTERVAL : one line per sample, local vs remote eth_blockNumber
L=$1; R=$2; T=${3:-600}; I=${4:-30}
bn() { curl -s -m 10 "$1" -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"],16))' 2>/dev/null || echo NA; }
end=$((SECONDS+T))
while [ $SECONDS -lt $end ]; do
  l=$(bn $L); r=$(bn $R); d=NA; [ "$l" != NA ] && [ "$r" != NA ] && d=$((r-l))
  echo "$(TZ=Asia/Tokyo date '+%H:%M:%S JST') local=$l remote=$r remote-local=$d"
  sleep $I
done
