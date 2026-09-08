#!/usr/bin/env python3
"""Measure one running subnetbench unit through its exact ready point."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.request


def rpc(url, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = urllib.request.Request(url, body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        answer = json.load(response)
    if "error" in answer:
        raise RuntimeError(answer["error"])
    if "result" not in answer:
        raise RuntimeError("RPC response has no result")
    return answer["result"]


def processes(cgroup):
    result = []
    for entry in sorted(cgroup.rglob("cgroup.procs")):
        for text in entry.read_text().split():
            pid = int(text)
            try:
                status = Path(f"/proc/{pid}/status").read_text()
                name = re.search(r"^Name:\s+(.*)$", status, re.M).group(1)
                values = {}
                for field in ("VmRSS", "VmHWM"):
                    match = re.search(rf"^{field}:\s+(\d+) kB$", status, re.M)
                    values[field] = int(match.group(1)) * 1024 if match else 0
                result.append({"pid": pid, "name": name, **values})
            except FileNotFoundError:
                continue
    return result


def disk_size(path, apparent):
    command = ["du", "-sb" if apparent else "-sB1", str(path)]
    return int(subprocess.check_output(command, text=True).split()[0])


def systemctl_value(unit, prop):
    return subprocess.check_output([
        "systemctl", "show", unit, f"--property={prop}", "--value"
    ], text=True).strip()


def positive_systemd_int(unit, prop, description):
    try:
        value = int(systemctl_value(unit, prop))
    except ValueError as err:
        raise SystemExit(f"unit has invalid {description}: {err}") from err
    if value <= 0:
        raise SystemExit(f"unit has no {description}")
    return value


def ready_marker(height, host_pid):
    return re.compile(rf"corpus ready height={height} host_pid={host_pid}(?:\s|$)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--rpc", required=True)
    parser.add_argument("--height", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=7200)
    parser.add_argument("--settle", type=float, default=10)
    args = parser.parse_args()
    started_us = positive_systemd_int(args.unit, "ExecMainStartTimestampMonotonic", "process start timestamp")
    main_pid = positive_systemd_int(args.unit, "MainPID", "main PID")
    started = started_us * 1000
    group = Path("/sys/fs/cgroup/system.slice") / args.unit
    marker = ready_marker(args.height, main_pid)
    offset = 0
    tail = ""
    ready = None
    peak_rss = 0
    process_hwm = {}
    last_sample = 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as output:
        def emit(row):
            output.write(json.dumps(row, sort_keys=True) + "\n")
            output.flush()

        emit({"type": "start", "unit": args.unit, "started_monotonic_ns": started,
              "main_pid": main_pid, "height": args.height,
              "sample_seconds": 1, "ready_poll_seconds": 0.1})
        while True:
            now = time.monotonic_ns()
            elapsed = (now - started) / 1e9
            if elapsed > args.timeout:
                raise TimeoutError(f"unit did not reach {args.height} within {args.timeout} seconds")
            if now - last_sample >= 1_000_000_000 or last_sample == 0:
                pids = processes(group)
                if not pids:
                    raise RuntimeError("contender exited before measurement completed")
                rss = sum(p["VmRSS"] for p in pids)
                peak_rss = max(peak_rss, rss)
                for p in pids:
                    process_hwm[p["pid"]] = max(process_hwm.get(p["pid"], 0), p["VmHWM"])
                emit({"type": "sample", "elapsed_seconds": elapsed,
                      "rss_bytes": rss, "processes": pids})
                last_sample = now
            if ready is None and args.log.exists():
                with args.log.open() as log:
                    log.seek(offset)
                    tail = (tail + log.read())[-16384:]
                    offset = log.tell()
                if marker.search(tail):
                    observed = int(rpc(args.rpc, "eth_blockNumber", []), 16)
                    # Timestamp after the response, never before its blocking call.
                    ready = time.monotonic_ns()
                    if observed != args.height:
                        raise RuntimeError(f"ready marker has RPC height {observed}, expected {args.height}")
                    emit({"type": "ready", "height": observed, "main_pid": main_pid,
                          "ready_seconds": (ready - started) / 1e9})
            if ready is not None and now - ready >= args.settle * 1e9:
                block = rpc(args.rpc, "eth_getBlockByNumber", [hex(args.height), False])
                if int(rpc(args.rpc, "eth_blockNumber", []), 16) != args.height:
                    raise RuntimeError("head changed after readiness")
                sizes = {}
                for label, path in [("total", args.data), ("block_database", args.data / "chainData/db"),
                                    ("firewood", args.data / "chainData/firewood"),
                                    ("epochdb", args.data / "chainData/epochdb")]:
                    if path.exists():
                        sizes[label] = {"apparent_bytes": disk_size(path, True),
                                        "allocated_bytes": disk_size(path, False)}
                summary = {"type": "summary", "height": args.height, "main_pid": main_pid,
                           "block_hash": block["hash"], "state_root": block["stateRoot"],
                           "ready_seconds": (ready - started) / 1e9,
                           "sampled_peak_rss_bytes": peak_rss,
                           "sum_process_hwm_bytes": sum(process_hwm.values()),
                           "settled_rss_bytes": sum(p["VmRSS"] for p in processes(group)),
                           "disk": sizes}
                emit(summary)
                print(json.dumps(summary, indent=2, sort_keys=True))
                return
            time.sleep(0.1)


if __name__ == "__main__":
    main()
