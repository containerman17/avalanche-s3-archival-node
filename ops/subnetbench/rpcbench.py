#!/usr/bin/env python3
"""Generate and replay a fixed workload against current subnet-evm state."""

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import http.client
import json
import math
from pathlib import Path
import random
import re
import sys
import time
from urllib.parse import urlsplit

METHODS = ("eth_getBalance", "eth_getStorageAt", "eth_call")
PER_METHOD = 1000
ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
HEX = re.compile(r"^0x[0-9a-fA-F]*$")


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def metadata_path(path):
    return Path(str(path) + ".meta.json")


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    with Path(path).open("x") as output:
        output.write(json.dumps(value, sort_keys=True, indent=2) + "\n")


def timestamp():
    return datetime.now(timezone(timedelta(hours=9))).isoformat()


class RPC:
    """One sequential HTTP connection. JSON encoding/decoding is not timed."""

    def __init__(self, url, timeout=5):
        parsed = urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username:
            raise ValueError("RPC URL must be an HTTP(S) URL without credentials")
        connection = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        self.connection = connection(parsed.hostname, parsed.port, timeout=timeout)
        self.path = parsed.path or "/"
        if parsed.query:
            self.path += "?" + parsed.query
        self.next_id = 0

    def close(self):
        self.connection.close()

    def request(self, method, params):
        self.next_id += 1
        body = encode({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params}).encode()
        started = time.perf_counter_ns()
        try:
            self.connection.request("POST", self.path, body=body, headers={"Content-Type": "application/json", "Connection": "keep-alive"})
            response = self.connection.getresponse()
            raw = response.read()
            elapsed = time.perf_counter_ns() - started
            status = response.status
        except (OSError, http.client.HTTPException) as exc:
            elapsed = time.perf_counter_ns() - started
            self.connection.close()
            return {"latency_ns": elapsed, "http_status": None, "response": None, "response_body": None, "transport_error": str(exc)}
        record = {"latency_ns": elapsed, "http_status": status, "response_body": raw.decode("utf-8", errors="replace")}
        try:
            record["response"] = json.loads(raw)
        except (ValueError, UnicodeDecodeError) as exc:
            record["response"] = None
            record["decode_error"] = str(exc)
        return record


def result(record):
    if record.get("transport_error") or record.get("decode_error"):
        raise ValueError(record.get("transport_error") or record.get("decode_error"))
    if record.get("http_status") != 200:
        raise ValueError(f"HTTP status {record.get('http_status')}")
    response = record.get("response")
    if not isinstance(response, dict) or response.get("jsonrpc") != "2.0":
        raise ValueError("invalid JSON-RPC response")
    if "error" in response:
        raise ValueError(f"JSON-RPC error: {encode(response['error'])}")
    if "result" not in response:
        raise ValueError("JSON-RPC result is missing")
    return response["result"]


def call(client, method, params):
    return result(client.request(method, params))


def head(client):
    chain_id = call(client, "eth_chainId", [])
    block = call(client, "eth_getBlockByNumber", ["latest", False])
    if not isinstance(block, dict) or not block.get("hash") or not block.get("number"):
        raise ValueError("latest block is missing its hash or number")
    return {"chain_id": chain_id, "head_hash": block["hash"], "height": int(block["number"], 16)}


def hex_value(value, allow_empty=False):
    if not isinstance(value, str) or not HEX.fullmatch(value) or (len(value) == 2 and not allow_empty):
        raise ValueError("RPC result is not a hex value")
    return int(value[2:] or "0", 16)


def probe(client, method, params, counters):
    counters[method] += 1
    try:
        return call(client, method, params)
    except ValueError:
        counters["probe_errors"] += 1
        return None


def generate(client, height, seed=17, window=10000, sample_blocks=128, max_contracts=16, budget=100):
    started = time.monotonic()
    baseline = head(client)
    if baseline["height"] != height:
        raise ValueError(f"RPC head {baseline['height']} differs from requested height {height}")
    rng = random.Random(seed)
    first = max(1, height - window + 1)
    candidates = list(range(first, height + 1))
    if not candidates:
        raise ValueError("generation requires a non-genesis head")
    sampled = sorted(rng.sample(candidates, min(sample_blocks, len(candidates))), reverse=True)
    if height not in sampled:
        sampled[-1] = height
        sampled.sort(reverse=True)
    counts, address_counts, target_counts = Counter(), Counter(), Counter()
    observed, scanned = [], []
    enough_time = lambda fraction: time.monotonic() < started + budget * fraction
    for number in sampled:
        if not enough_time(0.35):
            break
        block = call(client, "eth_getBlockByNumber", [hex(number), True])
        counts["eth_getBlockByNumber"] += 1
        if not isinstance(block, dict) or int(block.get("number", "-1"), 16) != number:
            raise ValueError(f"block input scan returned the wrong height for {number}")
        scanned.append(number)
        for tx in block.get("transactions", []):
            if not isinstance(tx, dict):
                raise ValueError("block input scan did not return full transactions")
            sender, target = tx.get("from"), tx.get("to")
            for address in (sender, target):
                if isinstance(address, str) and ADDRESS.fullmatch(address):
                    address_counts[address.lower()] += 1
            if isinstance(target, str) and ADDRESS.fullmatch(target):
                target = target.lower()
                target_counts[target] += 1
                data = tx.get("input", "0x")
                if isinstance(data, str) and HEX.fullmatch(data) and 10 <= len(data) <= 4098:
                    request = {"to": target, "data": data, "gas": "0x1e8480"}
                    if isinstance(sender, str) and ADDRESS.fullmatch(sender):
                        request["from"] = sender.lower()
                    if tx.get("value"):
                        request["value"] = tx["value"]
                    observed.append(request)
    if not address_counts:
        raise ValueError("sampled blocks contain no usable transaction addresses")
    addresses = sorted(address_counts)
    actors = rng.sample(addresses, min(4, len(addresses)))
    contracts = []
    for target in sorted(target_counts, key=lambda address: (-target_counts[address], address))[:96]:
        if not enough_time(0.50) or len(contracts) >= max_contracts:
            break
        code = probe(client, "eth_getCode", [target, "latest"], counts)
        if isinstance(code, str) and HEX.fullmatch(code) and code != "0x":
            contracts.append(target)
    if not contracts:
        raise ValueError("no code-bearing contract found in sampled transaction targets")

    # Compute Solidity mapping positions through Ethereum's Keccak RPC.
    mapping_keys = []
    for actor in actors:
        for slot in range(8):
            if not enough_time(0.60):
                break
            preimage = "0x" + actor[2:].rjust(64, "0") + f"{slot:064x}"
            key = probe(client, "web3_sha3", [preimage], counts)
            if isinstance(key, str) and re.fullmatch(r"0x[0-9a-fA-F]{64}", key):
                mapping_keys.append(key)
    populated, empty, storage_probes = [], [], []
    storage_kinds = Counter()
    for target in contracts:
        slots = [(hex(slot), "direct") for slot in range(32)] + [(key, "mapping_candidate") for key in mapping_keys]
        for key, kind in slots:
            if not enough_time(0.80):
                break
            params = [target, key, "latest"]
            value = probe(client, "eth_getStorageAt", params, counts)
            try:
                nonzero = hex_value(value) != 0
            except ValueError:
                continue
            (populated if nonzero else empty).append(params)
            storage_kinds[f"{kind}_{'nonzero' if nonzero else 'zero'}"] += 1
            storage_probes.append({"params": params, "kind": kind, "result": value})
    if not populated:
        raise ValueError("no populated current storage slots found; increase the block or contract sample")

    seen, call_pool, call_kinds, call_probes = set(), [], Counter(), []
    empty_call_results = 0
    call_candidates = [(request, "observed_transaction_input") for request in observed if request["to"] in contracts]
    rng.shuffle(call_candidates)
    call_candidates = call_candidates[:64]
    for target in contracts:
        for actor in actors:
            call_candidates.append(({"to": target, "from": actor, "data": "0x70a08231" + actor[2:].rjust(64, "0"), "gas": "0x1e8480"}, "balanceOf_candidate"))
        for selector, kind in (("0x18160ddd", "totalSupply_candidate"), ("0x313ce567", "decimals_candidate")):
            call_candidates.append(({"to": target, "data": selector, "gas": "0x1e8480"}, kind))
    for request, kind in call_candidates:
        if not enough_time(1):
            break
        key = encode(request)
        if key in seen:
            continue
        seen.add(key)
        value = probe(client, "eth_call", [request, "latest"], counts)
        try:
            hex_value(value, allow_empty=True)
        except ValueError:
            continue
        call_pool.append([request, "latest"])
        call_kinds[kind] += 1
        call_probes.append({"params": [request, "latest"], "kind": kind, "result": value})
        empty_call_results += value == "0x"
    if not call_pool:
        raise ValueError("no successful current eth_call on sampled contracts")

    requests = []
    for _ in range(PER_METHOD):
        requests.append({"method": "eth_getBalance", "params": [rng.choice(addresses), "latest"]})
    requested_nonzero = 0
    for _ in range(PER_METHOD):
        pool = populated if populated and (not empty or rng.random() < 0.7) else empty
        requested_nonzero += pool is populated
        requests.append({"method": "eth_getStorageAt", "params": rng.choice(pool)})
    for _ in range(PER_METHOD):
        requests.append({"method": "eth_call", "params": rng.choice(call_pool)})
    rng.shuffle(requests)
    after = head(client)
    if after != baseline:
        raise ValueError("chain/head changed during request generation")
    metadata = {
        "format": 1, **baseline, "created_jst": timestamp(), "seed": seed,
        "requests_per_method": PER_METHOD, "request_count": len(requests),
        "selection": {
            "method": "sampled transaction senders/targets and observed inputs, direct slots and mapping candidates, successful ABI selector candidates",
            "state_block": "latest", "block_window": [first, height], "sampled_heights": scanned,
            "requested_sample_blocks": sample_blocks, "unique_addresses": len(addresses),
            "contracts": contracts, "mapping_actors": actors, "storage_pool_nonzero": len(populated),
            "storage_pool_zero": len(empty), "storage_pool_kinds": dict(storage_kinds),
            "storage_requests_nonzero_at_selection": requested_nonzero,
            "storage_requests_zero_at_selection": PER_METHOD - requested_nonzero,
            "successful_call_pool": len(call_pool), "call_pool_kinds": dict(call_kinds),
            "call_pool_empty_results": empty_call_results,
            "storage_probe_results": storage_probes, "call_probe_results": call_probes,
            "probe_counts": dict(counts), "sampling_budget_seconds": budget,
            "elapsed_seconds": time.monotonic() - started,
            "unique_requests_per_method": {method: len({encode(request) for request in requests if request["method"] == method}) for method in METHODS},
        },
    }
    return requests, metadata


def save_requests(path, requests, metadata):
    path = Path(path)
    if path.exists() or metadata_path(path).exists():
        raise ValueError(f"output already exists: {path}")
    with path.open("x") as output:
        for request in requests:
            output.write(encode(request) + "\n")
    metadata = dict(metadata, requests_sha256=sha256(path))
    write_json(metadata_path(path), metadata)
    return metadata


def load_requests(path):
    metadata = json.loads(metadata_path(path).read_text())
    if sha256(path) != metadata["requests_sha256"]:
        raise ValueError("request file SHA-256 differs from metadata")
    requests = [json.loads(line) for line in Path(path).read_text().splitlines()]
    if len(requests) != metadata["request_count"]:
        raise ValueError("request count differs from metadata")
    for request in requests:
        if request.get("method") not in METHODS or not request.get("params") or request["params"][-1] != "latest":
            raise ValueError("workload contains a method or block tag outside current-state scope")
    if Counter(request["method"] for request in requests) != Counter({method: PER_METHOD for method in METHODS}):
        raise ValueError("workload must contain exactly 1000 requests per method")
    return requests, metadata


def replay(client, requests_path, output_path, label):
    requests, expected = load_requests(requests_path)
    pinned = {key: expected[key] for key in ("chain_id", "head_hash", "height")}
    before = head(client)
    if before != pinned:
        raise ValueError("chain/head before replay differs from request metadata")
    output_path = Path(output_path)
    if output_path.exists() or metadata_path(output_path).exists():
        raise ValueError(f"output already exists: {output_path}")
    with output_path.open("x") as output:
        for index, request in enumerate(requests):
            record = {"index": index, **request, **client.request(request["method"], request["params"])}
            output.write(encode(record) + "\n")
    metadata = {"format": 1, **pinned, "label": label, "created_jst": timestamp(),
                "requests_sha256": expected["requests_sha256"], "responses_sha256": sha256(output_path),
                "request_count": len(requests), "before": before, "head_valid": False}
    try:
        metadata["after"] = head(client)
        metadata["head_valid"] = metadata["after"] == pinned
    finally:
        write_json(metadata_path(output_path), metadata)
    if not metadata["head_valid"]:
        raise ValueError("chain/head changed during replay")
    return summarize(output_path)


def load_run(path):
    metadata = json.loads(metadata_path(path).read_text())
    if sha256(path) != metadata["responses_sha256"]:
        raise ValueError(f"response file SHA-256 differs from metadata: {path}")
    pinned = {key: metadata[key] for key in ("chain_id", "head_hash", "height")}
    if not metadata.get("head_valid") or metadata.get("before") != pinned or metadata.get("after") != pinned:
        raise ValueError(f"chain/head verification failed: {path}")
    records = [json.loads(line) for line in Path(path).read_text().splitlines()]
    if len(records) != metadata["request_count"]:
        raise ValueError(f"response count differs from metadata: {path}")
    for index, record in enumerate(records):
        if record.get("index") != index or record.get("method") not in METHODS:
            raise ValueError(f"invalid response sequence at index {index}: {path}")
    replayed_requests = "".join(encode({"method": record["method"], "params": record["params"]}) + "\n" for record in records)
    if hashlib.sha256(replayed_requests.encode()).hexdigest() != metadata["requests_sha256"]:
        raise ValueError(f"replayed requests differ from the fixed request file: {path}")
    return records, metadata


def successful(record):
    value = result(record)
    hex_value(value, allow_empty=record["method"] == "eth_call")
    return value


def percentile(values, quantile):
    return sorted(values)[max(0, math.ceil(len(values) * quantile) - 1)] if values else None


def summarize(path):
    records, metadata = load_run(path)
    summary = {"file": str(path), "label": metadata["label"], "requests_sha256": metadata["requests_sha256"],
               "chain_id": metadata["chain_id"], "head_hash": metadata["head_hash"], "height": metadata["height"],
               "methods": {}, "storage_nonzero": 0, "storage_zero": 0}
    for method in METHODS:
        latencies, errors = [], 0
        for record in records:
            if record["method"] != method:
                continue
            try:
                value = successful(record)
            except ValueError:
                errors += 1
                continue
            latencies.append(record["latency_ns"])
            if method == "eth_getStorageAt":
                summary["storage_nonzero" if hex_value(value) else "storage_zero"] += 1
        summary["methods"][method] = {"success": len(latencies), "error": errors,
                                      "p50_ns": percentile(latencies, 0.5), "p95_ns": percentile(latencies, 0.95)}
    summary["valid"] = all(stats["error"] == 0 for stats in summary["methods"].values())
    return summary


def normalized_response(record):
    successful(record)
    response = dict(record["response"])
    response.pop("id", None)
    return encode(response)


def compare(left_path, right_path):
    left, lm = load_run(left_path)
    right, rm = load_run(right_path)
    for key in ("requests_sha256", "chain_id", "head_hash", "height", "request_count"):
        if lm[key] != rm[key]:
            raise ValueError(f"runs differ in {key}")
    mismatches, examples = 0, []
    for index, (a, b) in enumerate(zip(left, right)):
        problem = None
        if (a["method"], a["params"]) != (b["method"], b["params"]):
            problem = "request differs"
        else:
            try:
                if normalized_response(a) != normalized_response(b):
                    problem = "response differs"
            except ValueError as exc:
                problem = str(exc)
        if problem:
            mismatches += 1
            if len(examples) < 20:
                examples.append({"index": index, "method": a["method"], "problem": problem})
    return {"equal": mismatches == 0, "mismatches": mismatches, "examples": examples,
            "left": summarize(left_path), "right": summarize(right_path)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    gen = commands.add_parser("generate", help="select 1000 current-state requests per method")
    gen.add_argument("--url", required=True)
    gen.add_argument("--height", type=int, required=True)
    gen.add_argument("--output", type=Path, required=True)
    gen.add_argument("--seed", type=int, default=17)
    gen.add_argument("--window", type=int, default=10000)
    gen.add_argument("--sample-blocks", type=int, default=128)
    gen.add_argument("--max-contracts", type=int, default=16)
    gen.add_argument("--budget-seconds", type=float, default=100)
    rep = commands.add_parser("replay", help="replay one pass with full responses and latency")
    rep.add_argument("--url", required=True)
    rep.add_argument("--requests", type=Path, required=True)
    rep.add_argument("--output", type=Path, required=True)
    rep.add_argument("--label", required=True)
    comp = commands.add_parser("compare", help="strict comparison and per-method summaries")
    comp.add_argument("left", type=Path)
    comp.add_argument("right", type=Path)
    comp.add_argument("--output", type=Path)
    summ = commands.add_parser("summarize", help="summarize a verified replay file")
    summ.add_argument("file", type=Path)
    summ.add_argument("--output", type=Path)
    args = parser.parse_args()
    client = None
    try:
        if args.output and (args.output.exists() or (args.command in ("generate", "replay") and metadata_path(args.output).exists())):
            raise ValueError(f"output already exists: {args.output}")
        if args.command in ("generate", "replay"):
            client = RPC(args.url)
        if args.command == "generate":
            if min(args.height, args.window, args.sample_blocks, args.max_contracts, args.budget_seconds) <= 0 or args.budget_seconds > 110:
                raise ValueError("sampling limits must be positive; budget must be at most 110 seconds")
            requests, metadata = generate(client, args.height, args.seed, args.window, args.sample_blocks, args.max_contracts, args.budget_seconds)
            report = save_requests(args.output, requests, metadata)
        elif args.command == "replay":
            report = replay(client, args.requests, args.output, args.label)
        elif args.command == "compare":
            report = compare(args.left, args.right)
        else:
            report = summarize(args.file)
        if args.command in ("compare", "summarize") and args.output:
            write_json(args.output, report)
        print(json.dumps(report, sort_keys=True, indent=2))
        return 0 if report.get("equal", report.get("valid", True)) else 1
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(f"rpcbench: {exc}", file=sys.stderr)
        return 1
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    sys.exit(main())
