import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("report.py")
SPEC = importlib.util.spec_from_file_location("subnetbench_report", SCRIPT)
report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(report)


def bench(elapsed, height, rate, wait=0, full=9.9, exit=False):
    tag = "exit " if exit else ""
    blocks = 100 if exit else 20
    return (f"2026/09/09 12:00:00 bench {tag}t={elapsed}s h={height} blk={blocks} tx=40 "
            f"mgas/s={rate} cum=250.0 wait={wait}s full={full}s host_rss=100MB vm_rss=200MB")


class ReportTest(unittest.TestCase):
    def setUp(self):
        self.corpus = {"format": "EPCORP01", "Blocks": 100, "Gas": 9_000_000_000,
                       "LastBlock": "0x" + "ab" * 32, "LastRoot": "0x" + "cd" * 32,
                       "SHA256": "ef" * 32}
        self.rows = [
            {"type": "start", "unit": "test.service", "main_pid": 123, "height": 100},
            {"type": "sample", "rss_bytes": 300, "elapsed_seconds": 2},
            {"type": "ready", "main_pid": 123, "height": 100, "ready_seconds": 50},
            {"type": "summary", "main_pid": 123, "height": 100, "ready_seconds": 50,
             "block_hash": self.corpus["LastBlock"], "state_root": self.corpus["LastRoot"],
             "sampled_peak_rss_bytes": 300, "sum_process_hwm_bytes": 320,
             "settled_rss_bytes": 280,
             "disk": {"total": {"apparent_bytes": 1000, "allocated_bytes": 4096},
                      "firewood": {"apparent_bytes": 500, "allocated_bytes": 4096}}},
        ]
        self.lines = [
            "epochdb-host: corpus=/fixture/corpus accepted=0 stop=100 batch=32 ring=1024 vm_pid=456",
            bench(9, 20, 999),
            bench(19, 40, 100),
            bench(29, 60, 300, wait=0.2, full=9.8),
            bench(39, 100, 999, wait=0.4, full=38.0, exit=True),
            "corpus ready height=100 host_pid=123 id=fixture vm_pid=456 rpc=http://localhost/rpc",
        ]

    def summarize(self):
        return report.summarize(self.rows, "\n".join(self.lines), self.corpus)

    def test_rates_resources_and_reproducible_samples(self):
        result = self.summarize()
        self.assertEqual(result["wall_mgas_per_second"], 180)
        self.assertEqual(result["last_cumulative_mgas_per_second"], 250)
        windows = result["complete_10_second_samples"]
        self.assertEqual(windows["count"], 2)
        self.assertEqual(windows["p50_mgas_per_second"], 200)
        self.assertAlmostEqual(windows["queue_wait_fraction"], 0.01)
        self.assertAlmostEqual(windows["ring_full_fraction"], 0.985)
        self.assertEqual(windows["samples_with_displayed_wait"], 1)
        self.assertEqual([row["elapsed_seconds"] for row in windows["rows"]], [19, 29])
        self.assertEqual(windows["rows"][0]["window_transactions"], 40)
        self.assertEqual(result["sampled_peak_rss_bytes"], 300)
        self.assertEqual(result["disk"]["total"]["allocated_bytes"], 4096)
        self.assertIn("not proof of zero wait", result["source_starvation_evidence"])

    def test_ansi_quoted_numeric_fields_and_duplicate_duration(self):
        self.lines[2] = "\x1b[32m" + self.lines[2].replace("t=19s", 't="1.9e1s"').replace(
            "mgas/s=100", 'mgas/s="1e2"') + "\x1b[0m"
        self.lines.insert(3, bench(19, 40, 99999))
        self.corpus["Gas"] = "9e9"
        self.rows[-1]["height"] = "1e2"
        result = self.summarize()
        windows = result["complete_10_second_samples"]
        self.assertEqual(windows["p50_mgas_per_second"], 200)
        self.assertEqual(windows["excluded"]["duplicate_duration"], 1)

    def test_missing_interval_and_after_exit_are_excluded(self):
        self.lines[3] = bench(35, 60, 99999)
        self.lines.insert(5, bench(45, 100, 99999))
        windows = self.summarize()["complete_10_second_samples"]
        self.assertEqual(windows["count"], 1)
        self.assertEqual(windows["p50_mgas_per_second"], 100)
        self.assertEqual(windows["excluded"]["non_10_second_gap"], 1)
        self.assertEqual(windows["excluded"]["after_exit"], 1)

    def test_reject_missing_measurement_records(self):
        original = self.rows
        for kind in ("start", "ready", "summary"):
            with self.subTest(kind=kind):
                self.rows = [row for row in original if row["type"] != kind]
                with self.assertRaisesRegex(ValueError, f"one {kind}"):
                    self.summarize()

    def test_reject_wrong_corpus_identity(self):
        original = copy.deepcopy(self.rows)
        for field, value in (("height", 99), ("block_hash", "0x" + "00" * 32),
                             ("state_root", "0x" + "00" * 32), ("main_pid", 321)):
            with self.subTest(field=field):
                self.rows = copy.deepcopy(original)
                self.rows[-1][field] = value
                with self.assertRaisesRegex(ValueError, "differs"):
                    self.summarize()

    def test_reject_incomplete_or_resumed_run(self):
        original = self.lines
        variants = [
            original[:-1],
            original[:4] + original[5:],
            original[:2] + original[4:],
            [original[0].replace("accepted=0", "accepted=1")] + original[1:],
            original[:4] + [original[4].replace("blk=100", "blk=99")] + original[5:],
        ]
        for lines in variants:
            with self.subTest(lines=lines):
                self.lines = lines
                with self.assertRaises(ValueError):
                    self.summarize()

    def test_reject_invalid_numbers(self):
        for value in ("NaN", "Infinity", "-1", "1e10000", True, ""):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    report.number(value, "fixture")

    def test_cli_writes_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "measurement.jsonl").write_text("\n".join(json.dumps(row) for row in self.rows))
            (path / "host.log").write_text("\n".join(self.lines))
            (path / "corpus.json").write_text(json.dumps(self.corpus))
            output = path / "report.json"
            subprocess.run([sys.executable, str(SCRIPT), "--measurement", str(path / "measurement.jsonl"),
                            "--log", str(path / "host.log"), "--corpus-metadata", str(path / "corpus.json"),
                            "--output", str(output)], check=True)
            self.assertEqual(json.loads(output.read_text())["wall_mgas_per_second"], 180)


if __name__ == "__main__":
    unittest.main()
