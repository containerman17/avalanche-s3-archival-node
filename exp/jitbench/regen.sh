#!/usr/bin/env bash
# Rebuilds exp/jitbench/vmx: libevm's core/vm (LGPL-3.0) copied verbatim out of
# the module cache, renamed to package vmx, plus the two files in patch/.
#
# The copy is NOT committed. epochdb is MIT and go-ethereum's core/vm is
# LGPL-3.0, so the fork is generated on demand instead of vendored.
#
#	./exp/jitbench/regen.sh && go test ./exp/jitbench/...
set -euo pipefail
cd "$(dirname "$0")"
L=$(cd ../.. && go list -m -f '{{.Dir}}' github.com/ava-labs/libevm)
rm -rf vmx && mkdir -p vmx/runtime

for f in "$L"/core/vm/*.go; do
	case $(basename "$f") in *_test.go) continue ;; esac
	cp "$f" vmx/
done
for f in "$L"/core/vm/runtime/*.go; do
	case $(basename "$f") in *_test.go) continue ;; esac
	cp "$f" vmx/runtime/
done
chmod -R u+w vmx

# package vm -> package vmx, and repoint the runtime harness at the fork.
sed -i 's/^package vm$/package vmx/' vmx/*.go
sed -i 's#"github.com/ava-labs/libevm/core/vm"#"github.com/containerman17/avalanche-s3-archival-node/exp/jitbench/vmx"#; s#\bvm\.#vmx.#g' vmx/runtime/*.go

# core.CanTransfer/Transfer are typed against libevm's own vm.StateDB, so the
# harness needs its own copies against the fork's.
python3 - <<'PY'
p = 'vmx/runtime/env.go'
s = open(p).read()
s = s.replace('"github.com/ava-labs/libevm/core"\n', '"github.com/ava-labs/libevm/common"\n\t"github.com/holiman/uint256"\n')
s = s.replace('CanTransfer: core.CanTransfer', 'CanTransfer: canTransfer')
s = s.replace('Transfer:    core.Transfer', 'Transfer:    transfer')
s += '''
func canTransfer(db vmx.StateDB, addr common.Address, amount *uint256.Int) bool {
	return db.GetBalance(addr).Cmp(amount) >= 0
}

func transfer(db vmx.StateDB, sender, recipient common.Address, amount *uint256.Int) {
	db.SubBalance(sender, amount)
	db.AddBalance(recipient, amount)
}
'''
open(p, 'w').write(s)

# The alternate run loops hang off Run, ahead of the stock loop. Tracing always
# takes the stock loop.
p = 'vmx/interpreter.go'
s = open(p).read()
anchor = '\tcontract.Input = input\n\n\tif debug {'
assert anchor in s, 'libevm changed: Run no longer has the contract.Input anchor'
s = s.replace(anchor, '''	contract.Input = input

	// EXPERIMENT: alternate run loops, see run_variants.go.
	if !debug {
		switch Mode {
		case ModeBlockGas:
			return in.runBlockGas(contract, callContext, stack, mem)
		case ModeNoMeter:
			return in.runNoMeter(contract, callContext, stack, mem)
		}
	}

	if debug {''', 1)
open(p, 'w').write(s)
PY

cat >> vmx/runtime/runtime.go <<'GO'

// SetDefaults exports setDefaults for the benchmark harness.
func SetDefaults(cfg *Config) { setDefaults(cfg) }
GO

for f in patch/*.go.txt; do cp "$f" "vmx/$(basename "$f" .txt)"; done
gofmt -w vmx/blockgas.go vmx/run_variants.go vmx/runtime/env.go vmx/interpreter.go
echo "vmx rebuilt from $L"
