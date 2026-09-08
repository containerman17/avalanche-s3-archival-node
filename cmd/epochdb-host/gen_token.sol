// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

// A realistic-ish ERC20: owner, pausable, frozen list, fee to a treasury,
// allowances. Every transfer reads owner/paused/fee config, two frozen flags,
// two or three balances, and writes two or three balances.
contract Token {
    string public constant name = "Bench";
    string public constant symbol = "BNCH";
    uint8 public constant decimals = 18;
    address public owner;
    address public treasury;
    bool public paused;
    uint256 public feeBps;
    uint256 public totalSupply;
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;
    mapping(address => bool) public frozen;

    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);

    constructor(address treasury_, uint256 feeBps_) {
        owner = msg.sender;
        treasury = treasury_;
        feeBps = feeBps_;
    }

    modifier onlyOwner() { require(msg.sender == owner, "owner"); _; }

    function mint(address to, uint256 amount) external onlyOwner {
        totalSupply += amount;
        balanceOf[to] += amount;
        emit Transfer(address(0), to, amount);
    }
    // mintMany seeds N fresh holders in one tx: holders are derived from seed.
    function mintMany(uint256 seed, uint256 n, uint256 amount) external onlyOwner {
        for (uint256 i = 0; i < n; i++) {
            address to = address(uint160(uint256(keccak256(abi.encode(seed, i)))));
            balanceOf[to] += amount;
        }
        totalSupply += n * amount;
    }
    function setFrozen(address who, bool f) external onlyOwner { frozen[who] = f; }
    function setPaused(bool p) external onlyOwner { paused = p; }

    function _transfer(address from, address to, uint256 amount) internal {
        require(!paused, "paused");
        require(!frozen[from] && !frozen[to], "frozen");
        uint256 fee = amount * feeBps / 10000;
        uint256 bal = balanceOf[from];
        require(bal >= amount, "balance");
        balanceOf[from] = bal - amount;
        balanceOf[to] += amount - fee;
        if (fee > 0) balanceOf[treasury] += fee;
        emit Transfer(from, to, amount);
    }
    function transfer(address to, uint256 amount) external returns (bool) {
        _transfer(msg.sender, to, amount);
        return true;
    }
    function approve(address spender, uint256 amount) external returns (bool) {
        allowance[msg.sender][spender] = amount;
        emit Approval(msg.sender, spender, amount);
        return true;
    }
    function transferFrom(address from, address to, uint256 amount) external returns (bool) {
        uint256 a = allowance[from][msg.sender];
        require(a >= amount, "allowance");
        allowance[from][msg.sender] = a - amount;
        _transfer(from, to, amount);
        return true;
    }
}
