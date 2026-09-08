"""Minimal RFC 6455 client (stdlib only): text frames, masking, close."""
import base64, json, os, socket, struct, time

class WS:
    def __init__(self, host, port, path, timeout=60):
        self.s = socket.create_connection((host, port), timeout=timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        self.s.sendall((f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                        f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            c = self.s.recv(4096)
            if not c: raise RuntimeError("closed during handshake")
            buf += c
        head, self.buf = buf.split(b"\r\n\r\n", 1)
        self.status = head.split(b"\r\n")[0].decode()
        if not self.status.startswith("HTTP/1.1 101"):
            raise RuntimeError(self.status + " " + self.buf.decode(errors="replace"))
    def _read(self, n):
        while len(self.buf) < n:
            c = self.s.recv(65536)
            if not c: raise EOFError
            self.buf += c
        out, self.buf = self.buf[:n], self.buf[n:]
        return out
    def send(self, op, payload=b""):
        if isinstance(payload, str): payload = payload.encode()
        n = len(payload)
        hdr = bytes([0x80 | op])
        if n < 126: hdr += bytes([0x80 | n])
        elif n < 65536: hdr += bytes([0x80 | 126]) + struct.pack(">H", n)
        else: hdr += bytes([0x80 | 127]) + struct.pack(">Q", n)
        mask = os.urandom(4)
        self.s.sendall(hdr + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))
    def _frame(self):
        b0, b1 = self._read(2)
        op, n = b0 & 0xf, b1 & 0x7f
        if n == 126: n = struct.unpack(">H", self._read(2))[0]
        elif n == 127: n = struct.unpack(">Q", self._read(8))[0]
        mask = self._read(4) if b1 & 0x80 else None
        p = self._read(n)
        if mask: p = bytes(b ^ mask[i % 4] for i, b in enumerate(p))
        return bool(b0 & 0x80), op, p
    def recv_frame(self):
        """One message: data frames reassembled over continuations (control frames may interleave)."""
        fin, op, p = self._frame()
        if op >= 8: return op, p
        while not fin:
            fin, cop, cp = self._frame()
            if cop >= 8:
                if cop == 9: self.send(10, cp)
                fin = False; continue
            p += cp
        return op, p
    def recv(self):
        """Next text message (answers pings, returns None on close)."""
        while True:
            op, p = self.recv_frame()
            if op == 1: return p.decode()
            if op == 9: self.send(10, p)
            elif op == 8:
                self.close_frame = p
                return None
    def call(self, method, params=None, id=1):
        self.send(1, json.dumps({"jsonrpc": "2.0", "id": id, "method": method, "params": params or []}))
        return json.loads(self.recv())
    def close(self):
        self.send(8, struct.pack(">H", 1000))
        try:
            while True:
                op, p = self.recv_frame()
                if op == 8: break
        except EOFError:
            p = None
        self.s.close()
        return p
