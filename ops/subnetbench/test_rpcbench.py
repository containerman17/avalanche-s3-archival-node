import hashlib
import http.server
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

import rpcbench


SENDER = "0x" + "11" * 20
OTHER = "0x" + "33" * 20
CONTRACT = "0x" + "22" * 20
HEAD = "0x" + "aa" * 32


class FakeRPC:
    def __init__(self, initial_id=0, drift=False):
        self.next_id = initial_id
        self.calls = []
        self.chain_reads = 0
        self.mapping_keys = {
            "0x" + hashlib.sha256(bytes.fromhex(address[2:].rjust(64, "0") + "00" * 32)).hexdigest()
            for address in (SENDER, OTHER, CONTRACT)
        }
        self.drift = drift

    def request(self, method, params):
        self.calls.append((method, params))
        self.next_id += 1
        error = None
        if method == "eth_chainId":
            self.chain_reads += 1
            value = "0x123"
        elif method == "eth_getBlockByNumber":
            if params[0] == "latest":
                value = {"number": "0x5", "hash": "0x" + "bb" * 32 if self.drift and self.chain_reads > 1 else HEAD}
            else:
                value = {"number": params[0], "transactions": [
                    {"from": SENDER, "to": CONTRACT, "input": "0xabcdef01", "value": "0x0"},
                    {"from": OTHER, "to": CONTRACT, "input": "0xdeadbeef", "value": "0x0"},
                ]}
        elif method == "eth_getCode":
            value = "0x6000" if params[0] == CONTRACT else "0x"
        elif method == "web3_sha3":
            value = "0x" + hashlib.sha256(bytes.fromhex(params[0][2:])).hexdigest()
        elif method == "eth_getStorageAt":
            value = "0x" + ("01" if params[1] == "0x0" or params[1] in self.mapping_keys else "00") * 32
        elif method == "eth_getBalance":
            value = "0x42"
        elif method == "eth_call":
            value = "0x" + "00" * 31 + "12"
            if params[0]["data"] == "0xdeadbeef":
                error = {"code": 3, "message": "execution reverted"}
        else:
            raise AssertionError(method)
        response = {"id": self.next_id, "jsonrpc": "2.0"}
        response["error" if error else "result"] = error if error else value
        return {"latency_ns": 1000 + self.next_id, "http_status": 200,
                "response": response, "response_body": json.dumps(response)}


class RPCBenchTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.requests, self.metadata = rpcbench.generate(FakeRPC(), 5, sample_blocks=3, max_contracts=2)
        self.requests_path = self.root / "requests.jsonl"
        rpcbench.save_requests(self.requests_path, self.requests, self.metadata)

    def test_generation_is_deterministic_and_current_only(self):
        client = FakeRPC()
        repeated, metadata = rpcbench.generate(client, 5, sample_blocks=3, max_contracts=2)
        self.assertEqual(self.requests, repeated)
        self.assertEqual(len(repeated), 3000)
        self.assertEqual({method: sum(request["method"] == method for request in repeated) for method in rpcbench.METHODS}, {method: 1000 for method in rpcbench.METHODS})
        for method, params in client.calls:
            if method in (*rpcbench.METHODS, "eth_getCode"):
                self.assertEqual(params[-1], "latest")
        self.assertGreater(metadata["selection"]["storage_pool_kinds"]["mapping_candidate_nonzero"], 0)
        self.assertGreater(metadata["selection"]["storage_requests_nonzero_at_selection"], 0)
        self.assertGreater(metadata["selection"]["storage_requests_zero_at_selection"], 0)
        self.assertGreater(metadata["selection"]["call_pool_kinds"]["observed_transaction_input"], 0)
        self.assertEqual(len(metadata["selection"]["storage_probe_results"]), sum(metadata["selection"]["storage_pool_kinds"].values()))
        self.assertEqual(len(metadata["selection"]["call_probe_results"]), metadata["selection"]["successful_call_pool"])
        self.assertNotIn("0xdeadbeef", {request["params"][0]["data"] for request in repeated if request["method"] == "eth_call"})

    def test_request_file_integrity(self):
        with self.requests_path.open("a") as output:
            output.write("{}\n")
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            rpcbench.load_requests(self.requests_path)

    def test_replay_and_strict_comparison(self):
        left, right = self.root / "a.jsonl", self.root / "b.jsonl"
        summary = rpcbench.replay(FakeRPC(), self.requests_path, left, "cold-a")
        rpcbench.replay(FakeRPC(initial_id=5000), self.requests_path, right, "cold-b")
        self.assertTrue(summary["valid"])
        self.assertEqual(summary["methods"]["eth_getStorageAt"]["success"], 1000)
        self.assertEqual(summary["storage_nonzero"], self.metadata["selection"]["storage_requests_nonzero_at_selection"])
        self.assertTrue(rpcbench.compare(left, right)["equal"])
        records, metadata = rpcbench.load_run(right)
        records[0]["response"]["result"] = "0x7"
        right.write_text("".join(rpcbench.encode(record) + "\n" for record in records))
        metadata["responses_sha256"] = rpcbench.sha256(right)
        rpcbench.metadata_path(right).write_text(json.dumps(metadata))
        report = rpcbench.compare(left, right)
        self.assertFalse(report["equal"])
        self.assertEqual(report["mismatches"], 1)

    def test_equal_errors_and_missing_results_still_fail(self):
        for bad in ({"jsonrpc": "2.0", "id": 1, "error": {"code": -1}}, {"jsonrpc": "2.0", "id": 1}):
            record = {"method": "eth_call", "http_status": 200, "response": bad}
            with self.assertRaises(ValueError):
                rpcbench.normalized_response(record)
        good = {"method": "eth_call", "http_status": 200, "response": {"jsonrpc": "2.0", "id": 1, "result": "0x01"}}
        other = {**good, "response": {"result": "0x01", "id": 99, "jsonrpc": "2.0"}}
        self.assertEqual(rpcbench.normalized_response(good), rpcbench.normalized_response(other))
        other["response"]["extra"] = {"id": "semantic"}
        self.assertNotEqual(rpcbench.normalized_response(good), rpcbench.normalized_response(other))

    def test_head_drift_invalidates_completed_replay(self):
        path = self.root / "drift.jsonl"
        with self.assertRaisesRegex(ValueError, "head changed"):
            rpcbench.replay(FakeRPC(drift=True), self.requests_path, path, "cold")
        self.assertEqual(len(path.read_text().splitlines()), 3000)
        self.assertFalse(json.loads(rpcbench.metadata_path(path).read_text())["head_valid"])
        with self.assertRaisesRegex(ValueError, "verification failed"):
            rpcbench.summarize(path)

    def test_replayed_params_are_bound_to_request_hash(self):
        path = self.root / "changed.jsonl"
        rpcbench.replay(FakeRPC(), self.requests_path, path, "warm")
        records, metadata = rpcbench.load_run(path)
        records[0]["params"][-1] = "0x1"
        path.write_text("".join(rpcbench.encode(record) + "\n" for record in records))
        metadata["responses_sha256"] = rpcbench.sha256(path)
        rpcbench.metadata_path(path).write_text(json.dumps(metadata))
        with self.assertRaisesRegex(ValueError, "fixed request file"):
            rpcbench.load_run(path)

    def test_existing_outputs_are_never_overwritten(self):
        before = self.requests_path.read_bytes()
        with self.assertRaisesRegex(ValueError, "already exists"):
            rpcbench.save_requests(self.requests_path, self.requests, self.metadata)
        with self.assertRaises(FileExistsError):
            rpcbench.write_json(self.requests_path, {})
        with self.assertRaisesRegex(ValueError, "already exists"):
            rpcbench.replay(FakeRPC(), self.requests_path, self.requests_path, "cold")
        self.assertEqual(self.requests_path.read_bytes(), before)
        orphan = self.root / "orphan.jsonl"
        rpcbench.write_json(rpcbench.metadata_path(orphan), {"keep": True})
        with self.assertRaisesRegex(ValueError, "already exists"):
            rpcbench.save_requests(orphan, self.requests, self.metadata)
        self.assertFalse(orphan.exists())


class HTTPClientTest(unittest.TestCase):
    def test_keepalive_and_request_body_latency(self):
        ports = []

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                ports.append(self.client_address[1])
                time.sleep(0.003)
                body = json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": "0x1"}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = rpcbench.RPC(f"http://127.0.0.1:{server.server_port}/rpc")
        try:
            for _ in range(2):
                record = client.request("eth_getBalance", [SENDER, "latest"])
                self.assertEqual(rpcbench.result(record), "0x1")
                self.assertGreaterEqual(record["latency_ns"], 3_000_000)
                self.assertEqual(json.loads(record["response_body"]), record["response"])
            self.assertEqual(len(set(ports)), 1)
        finally:
            client.close()
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
