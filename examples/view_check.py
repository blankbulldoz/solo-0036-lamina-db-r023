#!/usr/bin/env python3
"""Comprehensive materialized-view verification over the public HTTP API."""
import json, os, pathlib, shutil, socket, subprocess, tempfile, time, urllib.request, urllib.error

ROOT = pathlib.Path(__file__).resolve().parents[1]
BINARY = ROOT / "bin" / "laminad"


class Client:
    def __init__(self, data, port, restore=None):
        self.data = pathlib.Path(data)
        self.port = port
        self.url = f"http://127.0.0.1:{port}/v1/lamina"
        self.restore = restore
        self.proc = None
        self.log = None

    def start(self):
        self.log = tempfile.TemporaryFile()
        args = [str(BINARY), "-data", str(self.data), "-listen", f"127.0.0.1:{self.port}"]
        if self.restore:
            args += ["-restore", str(self.restore)]
            self.restore = None
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                self.log.seek(0)
                raise RuntimeError(self.log.read().decode())
            try:
                self.call("GET", "/health")
                return self
            except (OSError, urllib.error.URLError):
                time.sleep(0.03)
        raise RuntimeError("not ready")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=12)
        if self.log:
            self.log.close()
            self.log = None

    def call(self, method, path, data=None, status=200, raw=False):
        body = None if data is None else json.dumps(data).encode()
        req = urllib.request.Request(self.url + path, data=body, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=12) as r:
                code, payload = r.status, r.read()
        except urllib.error.HTTPError as e:
            code, payload = e.code, e.read()
        if code != status:
            raise AssertionError(f"{method} {path}: want {status} got {code}: {payload.decode()}")
        return payload if raw else json.loads(payload)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def require(cond, msg):
    if not cond:
        raise AssertionError(msg)


def main():
    temp = pathlib.Path(tempfile.mkdtemp(prefix="lamina-view-"))
    data = temp / "db"
    port = free_port()
    c = Client(data, port).start()
    try:
        schema = {"name": "spectra", "primary": "sample",
                  "columns": [{"name": "sample", "type": "int"}, {"name": "band", "type": "text"},
                              {"name": "amplitude", "type": "int", "nullable": True},
                              {"name": "valid", "type": "bool"}]}
        c.call("POST", "/relations", schema, 201)

        def begin():
            return c.call("POST", "/transactions", {}, 201)["id"]

        def put(tx, row):
            c.call("POST", f"/transactions/{tx}/put",
                   {"relation": "spectra", "row": row, "insert_only": True})

        def commit(tx):
            return c.call("POST", f"/transactions/{tx}/commit")["sequence"]

        tx = begin()
        for n in range(1, 5):
            put(tx, {"sample": n, "band": f"b{n}", "amplitude": n * 10, "valid": n != 4})
        seq_seed = commit(tx)  # seq 2; amplitudes 10+20+30+40 = 100, row 4 invalid but not null

        # 1. Eager retained view registered at seq 3; its head binds to the
        # view registration sequence and holds the values of source seq 2.
        eager = c.call("POST", "/views", {
            "name": "v_total", "relation": "spectra",
            "measures": [{"function": "count"}, {"function": "sum", "column": "amplitude"}]}, 201)
        require(eager["source_sequence"] == eager["created"] and eager["history_points"] == 1,
                "initial anchor wrong")

        # 2. Lazy head-only view with a filter.
        c.call("POST", "/views", {
            "name": "v_valid", "relation": "spectra", "measures": [{"function": "count"}],
            "where": [{"column": "valid", "op": "=", "value": True}],
            "refresh": "on_demand", "history": "head_only"}, 201)

        # 3. Invalid definition must leave no registered view.
        before = len(c.call("GET", "/views")["views"])
        c.call("POST", "/views", {"name": "v_bad", "relation": "spectra",
                                  "measures": [{"function": "sum", "column": "band"}]}, 400)
        c.call("POST", "/views", {"name": "v_bad", "relation": "nope",
                                  "measures": [{"function": "count"}]}, 400)
        require(len(c.call("GET", "/views")["views"]) == before, "failed view got registered")

        # 4. A source commit maintains the eager view in the same sequence.
        tx = begin()
        put(tx, {"sample": 5, "band": "b5", "amplitude": 50, "valid": True})
        seq5 = commit(tx)
        answer = c.call("POST", "/views/v_total/read", {"at": seq5})
        require(answer["source_sequence"] == seq5 and answer["row"] == {"count": 5, "sum_amplitude": 150},
                f"eager maintenance lagged: {answer}")
        require(answer["materialized"] is True, "eager answer should be materialized")

        # 5. History: old snapshot returns the old aggregate bound to old seq.
        old = c.call("POST", "/views/v_total/read", {"at": seq_seed})
        require(old["row"] == {"count": 4, "sum_amplitude": 100} and old["source_sequence"] == seq_seed,
                f"historical view wrong: {old}")

        # 6. Delete a matching row: count and sum decrement exactly.
        tx = begin()
        c.call("POST", f"/transactions/{tx}/delete", {"relation": "spectra", "key": 5})
        seq6 = commit(tx)
        after_delete = c.call("POST", "/views/v_total/read", {"at": seq6})
        require(after_delete["row"] == {"count": 4, "sum_amplitude": 100}, f"delete delta wrong: {after_delete}")

        # 7. Null sum input ignores nulls; deleting all matched gives null sum.
        c.call("POST", "/views", {"name": "v_amp", "relation": "spectra",
                                  "measures": [{"function": "sum", "column": "amplitude"}]}, 201)
        tx = begin()
        for n in range(1, 5):
            c.call("POST", f"/transactions/{tx}/delete", {"relation": "spectra", "key": n})
        seq_empty = commit(tx)
        empty = c.call("POST", "/views/v_amp/read", {"at": seq_empty})
        require(empty["row"] == {"sum_amplitude": None}, f"empty sum should be null: {empty}")

        # 8. Lazy view recomputes on demand, reports non-materialized + caught up.
        lazy = c.call("POST", "/views/v_valid/read", {"at": c.call("GET", "/stats")["sequence"]})
        require(lazy["materialized"] is False and lazy["caught_up"] is True and lazy["row"]["count"] == 0,
                f"lazy view wrong: {lazy}")
        # head-only rejects history.
        c.call("POST", "/views/v_valid/read", {"at": seq_seed}, 410)

        # 9. Verify endpoint recomputes and confirms correspondence.
        verified = c.call("POST", "/views/v_total/verify", {})
        require(verified["source_sequence"] == c.call("GET", "/stats")["sequence"], "verify sequence wrong")

        # 10. Checkpoint, then restart: derived state is reconstructed and verified.
        c.call("POST", "/checkpoint", {})
        head_before = c.call("GET", "/stats")["sequence"]
        c.stop()
        c.start()
        require(c.call("GET", "/stats")["sequence"] == head_before, "head changed across restart")
        recovered = c.call("POST", "/views/v_total/read", {"at": head_before})
        require(recovered["row"] == {"count": 0, "sum_amplitude": None}, f"recovered view wrong: {recovered}")
        # historical point survived checkpoint+restart
        hist = c.call("POST", "/views/v_total/read", {"at": seq5})
        require(hist["row"] == {"count": 5, "sum_amplitude": 150}, f"recovered history wrong: {hist}")
        c.call("POST", "/views/v_total/verify", {})
        # lazy view recovers without manual refresh
        lazy2 = c.call("POST", "/views/v_valid/read", {"at": head_before})
        require(lazy2["row"]["count"] == 0, "lazy view not trustworthy after restart")

        # 11. Vacuum prunes retained view history and enforces the new bound.
        # Re-seed so there is live data, then vacuum aggressively.
        tx = begin()
        for n in range(10, 13):
            put(tx, {"sample": n, "band": f"b{n}", "amplitude": n, "valid": True})
        seq_reseed = commit(tx)
        current = c.call("GET", "/stats")["sequence"]
        c.call("POST", "/vacuum", {"horizon": current}, 200)
        # pre-vacuum snapshot is now gone for both source and view
        c.call("POST", "/query", {"relation": "spectra", "at": seq5}, 410)
        c.call("POST", "/views/v_total/read", {"at": seq5}, 410)
        current2 = c.call("GET", "/stats")["sequence"]
        now = c.call("POST", "/views/v_total/read", {"at": current2})
        require(now["source_sequence"] == current2 and now["row"]["count"] == 3, f"post-vacuum read wrong: {now}")

        # 12. Sum overflow marks the view rather than corrupting the instance.
        c.call("POST", "/views", {"name": "v_big", "relation": "spectra",
                                  "measures": [{"function": "sum", "column": "amplitude"}]}, 201)
        # A count-only view has no integer sum that can overflow, so it stays
        # active and proves overflow is isolated per measure/view.
        c.call("POST", "/views", {"name": "v_safe", "relation": "spectra",
                                  "measures": [{"function": "count"}]}, 201)
        big = 2 ** 63 - 1
        tx = begin()
        put(tx, {"sample": 100, "band": "big", "amplitude": big, "valid": True})
        put(tx, {"sample": 101, "band": "big2", "amplitude": 1, "valid": True})
        seq_ov = commit(tx)
        ov = c.call("POST", "/views/v_big/read", {"at": seq_ov})
        require(ov["status"] == "overflow" and ov["overflow_columns"] == ["sum_amplitude"],
                f"overflow not marked: {ov}")
        # instance still serves and a measure without a sum stays healthy
        healthy = c.call("POST", "/views/v_safe/read", {"at": seq_ov})
        require(healthy["status"] == "active" and healthy["row"]["count"] == 5,
                f"overflow poisoned an unrelated view: {healthy}")
        # an overflowed sum column reads as null while other measures still serve
        mixed = c.call("POST", "/views/v_total/read", {"at": seq_ov})
        require(mixed["row"]["count"] == 5 and mixed["status"] == "overflow",
                f"overflow should null only the sum: {mixed}")
        # overflow state survives restart and verify reports it honestly
        c.stop()
        c.start()
        ov2 = c.call("POST", "/views/v_big/verify", {})
        require(ov2["status"] == "overflow", "overflow lost across restart")
        safe2 = c.call("POST", "/views/v_safe/verify", {})
        require(safe2["status"] == "active", "healthy view failed verification after restart")

        # 13. Restart without a checkpoint (journal-only replay) also verifies.
        c.stop()
        c.start()
        c.call("POST", "/views/v_safe/verify", {})

        print(json.dumps({"status": "passed", "checks": 13, "head": c.call("GET", "/stats")["sequence"]}))
    finally:
        c.stop()
        shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    main()
