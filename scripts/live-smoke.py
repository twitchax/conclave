#!/usr/bin/env python3
"""uat-005 live smoke (PRD-0009): two bridges on a deployed server exchange a channel message + whisper.

Usage: CONCLAVE_SMOKE_SERVER=wss://your-app.example conclave-repo$ cargo make uat-deploy-smoke
Requires: a registered identity in ~/.config/conclave for that server, `converse` perms on #ops and
whispers, and a built release binary (cargo make build-release).
"""
import json
import subprocess
import sys
import threading
import queue
import time

import os
import pathlib

BIN = str(pathlib.Path(__file__).resolve().parent.parent / "target" / "release" / "conclave")
SERVER = os.environ.get("CONCLAVE_SMOKE_SERVER") or sys.exit("set CONCLAVE_SMOKE_SERVER=wss://<your-server>")


class Bridge:
    def __init__(self, session):
        self.session = session
        self.proc = subprocess.Popen(
            [BIN, "bridge", "--server", SERVER, "--as", session],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        self.q = queue.Queue()
        threading.Thread(target=self._pump, daemon=True).start()
        self.next_id = 0

    def _pump(self):
        for line in self.proc.stdout:
            line = line.strip()
            if line:
                try:
                    self.q.put(json.loads(line))
                except json.JSONDecodeError:
                    pass

    def send(self, msg):
        self.proc.stdin.write(json.dumps(msg) + "\n")
        self.proc.stdin.flush()

    def request(self, method, params=None):
        self.next_id += 1
        self.send({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params or {}})
        return self.next_id

    def wait_for(self, pred, timeout=20):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = self.q.get(timeout=max(0.1, deadline - time.time()))
            except queue.Empty:
                break
            if pred(msg):
                return msg
        return None

    def call_tool(self, name, args, timeout=20):
        rid = self.request("tools/call", {"name": name, "arguments": args})
        return self.wait_for(lambda m: m.get("id") == rid, timeout)

    def close(self):
        self.proc.terminate()


def text_of(result):
    try:
        return result["result"]["content"][0]["text"]
    except (KeyError, IndexError, TypeError):
        return json.dumps(result)


def main():
    a = Bridge("smoke-a")
    b = Bridge("smoke-b")
    ok = True
    try:
        for br in (a, b):
            rid = br.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}})
            init = br.wait_for(lambda m, r=rid: m.get("id") == r)
            assert init, f"{br.session}: no initialize response"
            br.send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        # Both join #ops (deferred until the server's Joined ack — confirmed live).
        for br in (a, b):
            res = br.call_tool("join_channel", {"channel": "ops"})
            t = text_of(res)
            print(f"[{br.session}] join_channel -> {t}")
            ok &= res is not None and "joined ops" in t

        # A sends; the send is server-acked (confirmed delivery, PRD-0008 T-001)...
        res = a.call_tool("send_channel", {"channel": "ops", "text": "hello from smoke-a over Fly TLS"})
        t = text_of(res)
        print(f"[smoke-a] send_channel -> {t}")
        ok &= res is not None and "sent to ops" in t

        # ...and B receives the injected notification.
        note = b.wait_for(lambda m: m.get("method") == "notifications/claude/channel")
        if note:
            content = note["params"]["content"]
            frm = note["params"].get("meta", {}).get("from", "?")
            print(f"[smoke-b] received channel notification from {frm}: {content.splitlines()[-2] if len(content.splitlines()) > 1 else content}")
            ok &= "hello from smoke-a over Fly TLS" in content
        else:
            print("[smoke-b] NO channel notification received")
            ok = False

        # Whisper B -> A by full path (derived from live presence, also server-acked).
        res = a.call_tool("who", {"channel": "ops"})
        paths = [p.strip() for p in text_of(res).split(":", 1)[-1].split(",")]
        target = next(p for p in paths if p.endswith("/smoke-a"))
        res = b.call_tool("whisper", {"target": target, "text": "psst, whisper over the live server"})
        t = text_of(res)
        print(f"[smoke-b] whisper -> {t}")
        ok &= res is not None and "whisper sent" in t

        note = a.wait_for(lambda m: m.get("method") == "notifications/claude/channel")
        if note and "psst, whisper over the live server" in note["params"]["content"]:
            print(f"[smoke-a] received whisper: kind={note['params'].get('meta', {}).get('kind')}")
        else:
            print(f"[smoke-a] NO whisper received: {note}")
            ok = False

        # Presence: both live sessions visible in #ops.
        res = a.call_tool("who", {"channel": "ops"})
        t = text_of(res)
        print(f"[smoke-a] who(ops) -> {t}")
        ok &= "smoke-a" in t and "smoke-b" in t

    finally:
        a.close()
        b.close()

    print("SMOKE:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
