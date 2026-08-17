package jitbench

import "bytes"

// bodies are stack-neutral loop bodies. They cover the four things a replay
// actually burns time on and the three opcode classes that make basic-block
// gas accounting hard (GAS, the CALL family, SSTORE all read the remaining
// gas mid-block).
var bodies = map[string][]byte{
	// Pure 256-bit arithmetic: the best case for any compiler, since every
	// operand could live in a register instead of on a heap-backed stack.
	// The operands come off the loop counter (DUP1) so nothing here can be
	// constant-folded: a compiler has to do the arithmetic, same as the
	// interpreter, and only the machinery around it differs.
	"arith": bytes.Repeat([]byte{
		0x80,       // DUP1  (the loop counter)
		0x60, 0x07, // PUSH1 7
		0x01,       // ADD
		0x60, 0x03, // PUSH1 3
		0x02, // MUL
		0x50, // POP
	}, 10),
	// Hashing: the work is inside a precompiled Go routine, so a compiler can
	// only remove the dispatch around it.
	"keccak": {
		0x60, 0x20, // PUSH1 32
		0x60, 0x00, // PUSH1 0
		0x20, // SHA3
		0x50, // POP
	},
	// Memory traffic.
	"memory": {
		0x60, 0xff, // PUSH1 255
		0x60, 0x40, // PUSH1 64
		0x52, // MSTORE
	},
	// GAS reads the remaining gas: the correction path.
	"gasop": {
		0x5a, // GAS
		0x50, // POP
	},
	// Warm SLOAD: a host call, uncompilable by definition.
	"sload": {
		0x80, // DUP1
		0x54, // SLOAD
		0x50, // POP
	},
	// STATICCALL to the identity precompile: the 63/64 rule reads the
	// remaining gas, so this is the other correction path.
	"staticcall": {
		0x60, 0x00, // PUSH1 0   retSize
		0x60, 0x00, // PUSH1 0   retOffset
		0x60, 0x00, // PUSH1 0   argsSize
		0x60, 0x00, // PUSH1 0   argsOffset
		0x60, 0x04, // PUSH1 4   identity precompile
		0x5a, // GAS
		0xfa, // STATICCALL
		0x50, // POP
	},
	// SSTORE reads the remaining gas for the EIP-2200 reentrancy sentry.
	"sstore": {
		0x80, // DUP1
		0x80, // DUP1
		0x55, // SSTORE
	},
}

// loopN wraps a stack-neutral body in a counted loop:
//
//	PUSH2 n; JUMPDEST; <body>; PUSH1 1; SWAP1; SUB; DUP1; PUSH2 3; JUMPI; STOP
func loopN(n uint16, body []byte) []byte {
	out := []byte{0x61, byte(n >> 8), byte(n), 0x5b} // PUSH2 n, JUMPDEST at pc 3
	out = append(out, body...)
	out = append(out,
		0x60, 0x01, // PUSH1 1
		0x90,             // SWAP1
		0x03,             // SUB
		0x80,             // DUP1
		0x61, 0x00, 0x03, // PUSH2 3
		0x57, // JUMPI
		0x00, // STOP
	)
	return out
}

// kernels are the short versions the exactness sweep runs a thousand times.
var kernels = func() map[string][]byte {
	m := map[string][]byte{}
	for name, b := range bodies {
		n := uint16(40)
		if name == "sstore" {
			n = 6 // 20k gas apiece
		}
		m[name] = loopN(n, b)
	}
	return m
}()

// benchKernels are the long versions, sized so each is a few hundred
// microseconds of pure interpretation.
var benchKernels = func() map[string][]byte {
	m := map[string][]byte{}
	for name, b := range bodies {
		n := uint16(20000)
		switch name {
		case "sstore":
			n = 1000
		case "keccak", "staticcall":
			n = 10000
		}
		m[name] = loopN(n, b)
	}
	return m
}()
