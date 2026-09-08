#!/usr/bin/env python3
"""Report one complete fixed-corpus run from measure.py JSONL and host logs."""

import argparse
from decimal import Decimal, InvalidOperation
import json
import math
from pathlib import Path
import re
import statistics


ANSI = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))")
FIELDS = re.compile(r'''([\w/]+)=(?:"([^"]*)"|'([^']*)'|(\S+))''')


def fields(text):
    return {match[0]: match[1] or match[2] or match[3]
            for match in FIELDS.findall(text)}


def number(value, name, integer=False, suffix=""):
    text = str(value)
    if suffix and text.endswith(suffix):
        text = text[:-len(suffix)]
    try:
        parsed = Decimal(text)
    except InvalidOperation as err:
        raise ValueError(f"invalid {name}: {value!r}") from err
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"invalid {name}: {value!r}")
    if integer:
        if parsed != parsed.to_integral_value():
            raise ValueError(f"noninteger {name}: {value!r}")
        return int(parsed)
    result = float(parsed)
    if not math.isfinite(result):
        raise ValueError(f"invalid {name}: {value!r}")
    return result


def hash_value(value, name):
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
        raise ValueError(f"invalid {name}")
    return value.lower()


def summarize(rows, log, corpus):
    if corpus["format"] != "EPCORP01":
        raise ValueError("unsupported corpus format")
    height = number(corpus["Blocks"], "corpus Blocks", integer=True)
    gas = number(corpus["Gas"], "corpus Gas", integer=True)
    block_hash = hash_value(corpus["LastBlock"], "corpus LastBlock")
    state_root = hash_value(corpus["LastRoot"], "corpus LastRoot")
    if height == 0:
        raise ValueError("corpus has no blocks")
    records = {}
    for kind in ("start", "ready", "summary"):
        matches = [row for row in rows if row.get("type") == kind]
        if len(matches) != 1:
            raise ValueError(f"measurement requires exactly one {kind} record")
        records[kind] = matches[0]
    start, ready, summary = (records[kind] for kind in ("start", "ready", "summary"))
    pid = number(start["main_pid"], "main_pid", integer=True)
    if pid == 0:
        raise ValueError("main_pid must be positive")
    for kind, row in records.items():
        if number(row["height"], f"{kind} height", integer=True) != height:
            raise ValueError(f"{kind} height differs from corpus")
        if number(row["main_pid"], f"{kind} main_pid", integer=True) != pid:
            raise ValueError(f"{kind} main_pid differs from start")
    for name, expected in (("block_hash", block_hash), ("state_root", state_root)):
        if hash_value(summary[name], name) != expected:
            raise ValueError(f"summary {name} differs from corpus")
    seconds = number(ready["ready_seconds"], "ready_seconds")
    if seconds <= 0 or number(summary["ready_seconds"], "summary ready_seconds") != seconds:
        raise ValueError("ready and summary require the same positive ready_seconds")

    lines = ANSI.sub("", log).splitlines()
    marker = re.compile(rf"corpus ready height={height} host_pid={pid}(?:\s|$)")
    ends = [i for i, line in enumerate(lines) if marker.search(line)]
    if not ends:
        raise ValueError("host log has no matching ready marker")
    end = ends[0]
    beginnings = [i for i, line in enumerate(lines[:end]) if "epochdb-host: corpus=" in line]
    if not beginnings:
        raise ValueError("host log has no corpus start")
    beginning = beginnings[-1]
    launch = fields(lines[beginning])
    if number(launch["accepted"], "initial accepted height", integer=True) != 0:
        raise ValueError("corpus run resumed above genesis; total corpus gas is not applicable")
    if number(launch["stop"], "host stop", integer=True) != height:
        raise ValueError("host stop differs from corpus")

    samples, exits = [], []
    excluded = {"first_partial": 0, "duplicate_duration": 0, "non_10_second_gap": 0, "after_exit": 0}
    previous = None
    seen = set()
    for line in lines[beginning + 1:end]:
        match = re.search(r"\bbench\s+(exit\s+)?(.*)", line)
        if not match:
            continue
        raw = fields(match[2])
        sample = {
            "elapsed": number(raw["t"], "bench t", integer=True, suffix="s"),
            "height": number(raw["h"], "bench h", integer=True),
            "blocks": number(raw["blk"], "bench blk", integer=True),
            "transactions": number(raw["tx"], "bench tx", integer=True),
            "rate": number(raw["mgas/s"], "bench mgas/s"),
            "cumulative": number(raw["cum"], "bench cum"),
            "wait": number(raw["wait"], "bench wait", suffix="s"),
            "full": number(raw["full"], "bench full", suffix="s"),
        }
        if sample["height"] > height:
            raise ValueError("bench height exceeds corpus")
        if match[1]:
            exits.append(sample)
            continue
        if exits:
            excluded["after_exit"] += 1
            continue
        elapsed = sample["elapsed"]
        if elapsed in seen:
            excluded["duplicate_duration"] += 1
            continue
        seen.add(elapsed)
        if previous is None:
            excluded["first_partial"] += 1
        elif elapsed < previous:
            raise ValueError("bench elapsed time moved backwards")
        elif elapsed - previous != 10:
            excluded["non_10_second_gap"] += 1
        else:
            samples.append(sample)
        previous = elapsed
    if not exits:
        raise ValueError("host log has no bench exit summary")
    last = exits[-1]
    if last["height"] != height or last["blocks"] != height:
        raise ValueError("bench exit did not replay the complete corpus")
    if last["elapsed"] <= 0 or last["elapsed"] > seconds:
        raise ValueError("bench exit duration is inconsistent with readiness")
    if not samples:
        raise ValueError("host log has no complete 10-second samples")
    if samples[-1]["elapsed"] > last["elapsed"]:
        raise ValueError("bench sample extends beyond exit")

    window_seconds = len(samples) * 10
    wait = sum(sample["wait"] for sample in samples)
    full = sum(sample["full"] for sample in samples)
    rss = {name: number(summary[name], name, integer=True) for name in (
        "sampled_peak_rss_bytes", "sum_process_hwm_bytes", "settled_rss_bytes")}
    disk = {label: {name: number(sizes[name], f"disk {label} {name}", integer=True)
                    for name in ("apparent_bytes", "allocated_bytes")}
            for label, sizes in summary["disk"].items()}
    if "total" not in disk:
        raise ValueError("measurement has no total disk size")
    return {
        "unit": start["unit"], "main_pid": pid,
        "corpus": {"blocks": height, "gas": gas, "block_hash": block_hash,
                   "state_root": state_root, "sha256": corpus.get("SHA256")},
        "ready_seconds": seconds,
        "wall_mgas_per_second": gas / 1_000_000 / seconds,
        "complete_10_second_samples": {
            "count": len(samples),
            "p50_mgas_per_second": statistics.median(sample["rate"] for sample in samples),
            "queue_wait_seconds": wait, "queue_wait_fraction": wait / window_seconds,
            "ring_full_seconds": full, "ring_full_fraction": full / window_seconds,
            "samples_with_displayed_wait": sum(sample["wait"] > 0 for sample in samples),
            "excluded": excluded,
            "rows": [{
                "elapsed_seconds": sample["elapsed"], "height": sample["height"],
                "window_blocks": sample["blocks"], "window_transactions": sample["transactions"],
                "mgas_per_second": sample["rate"],
                "cumulative_mgas_per_second": sample["cumulative"],
                "queue_wait_seconds": sample["wait"], "ring_full_seconds": sample["full"],
            } for sample in samples],
        },
        "last_cumulative_mgas_per_second": last["cumulative"],
        "bench_exit": {
            "elapsed_seconds": last["elapsed"],
            "queue_wait_seconds": last["wait"],
            "queue_wait_fraction": last["wait"] / last["elapsed"],
            "ring_full_seconds": last["full"],
            "ring_full_fraction": last["full"] / last["elapsed"],
        },
        "source_starvation_evidence": (
            "Queue wait measures time waiting for the next parsed batch, including input and parsing delays. "
            "Ring-full time samples the host input ring at capacity every 100 ms. "
            "Wait/full values are rounded to 0.1 seconds; displayed 0.0 is not proof of zero wait. "
            "No starvation threshold or bottleneck classification is applied."
        ),
        "precision": "Bench gas rates are rounded to 0.1 Mgas/s; elapsed seconds are truncated to integers.",
        "rss_scope": "All measured host and VM processes; summed process high-water marks need not occur together.",
        **rss, "disk": disk,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurement", type=Path, required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--corpus-metadata", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        rows = [json.loads(line) for line in args.measurement.read_text().splitlines() if line.strip()]
        report = summarize(rows, args.log.read_text(), json.loads(args.corpus_metadata.read_text()))
        encoded = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
        if args.output:
            args.output.write_text(encoded)
        else:
            print(encoded, end="")
    except (OSError, ValueError, KeyError, TypeError) as err:
        parser.error(str(err))


if __name__ == "__main__":
    main()
