// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

// The Clear Street settlement shape without a precompile: every row moves
// qty of one security from one party to another and mirrors the move on the
// two holders' omnibus balances. Holder = party >> 4 (16 parties per holder),
// so no lookup slot is read. Arithmetic wraps: the benchmark never wants a
// revert, only the state writes.
contract Settle {
    mapping(uint256 => uint256) public shadow; // key (sec << 16) | party
    mapping(uint256 => uint256) public omnibus; // key (sec << 16) | holder

    function credit(uint256 sec, uint256 fromParty, uint256 count, uint256 amount) external {
        unchecked {
            for (uint256 p = fromParty; p < fromParty + count; p++) {
                shadow[(sec << 16) | p] += amount;
                omnibus[(sec << 16) | (p >> 4)] += amount;
            }
        }
    }

    // rows: 8 bytes each, big-endian: sec u8, from u16, to u16, qty u24.
    function settle(bytes calldata rows) external {
        unchecked {
            for (uint256 i = 0; i < rows.length; i += 8) {
                uint256 r = uint256(uint64(bytes8(rows[i:i + 8])));
                uint256 sec = r >> 56;
                uint256 from = (r >> 40) & 0xffff;
                uint256 to = (r >> 24) & 0xffff;
                uint256 qty = r & 0xffffff;
                shadow[(sec << 16) | from] -= qty;
                shadow[(sec << 16) | to] += qty;
                omnibus[(sec << 16) | (from >> 4)] -= qty;
                omnibus[(sec << 16) | (to >> 4)] += qty;
            }
        }
    }
}
