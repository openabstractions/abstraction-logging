"""conformance/faults/sink-outage through the Python ServiceHandler [LOG-S12, LOG-S13].

Every language runs the same steps and asserts the same counts, transitions,
delivered order, gap record and default reporter lines.
"""
import contextlib
import io
import json
import logging
import re
import threading
import time
import unittest
from pathlib import Path

from abstraction.logging.handler import ServiceHandler, failure_class, BACKOFF_JITTER, GAP_MESSAGE

FIXTURE = Path(__file__).resolve().parents[3] / "conformance" / "faults" / "sink-outage" / "sink-outage.json"
INSTANT = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$")


class ResolutionError(RuntimeError):
    """The facade's resolution error, in the shape failure_class reads."""

    def __init__(self, status, cause):
        super().__init__("service resolution: %s: abstraction.logging/sink@1 (capability abstraction.logging) at "
                         "the installed runtime: %s" % (status, cause))
        self.status, self.contract = status, "abstraction.logging/sink@1"


class OutageSink:
    def __init__(self, outage):
        self.outage = outage
        self.down = False
        self.delivered = []
        self.rebinds = 0
        self.lock = threading.Lock()
        self.hold = None
        self.held = False
        self.release = threading.Event()

    def write(self, record):
        with self.lock:
            if self.down:
                raise ConnectionError(self.outage["write_error"])
            wait = self.hold is not None and record.msg == self.hold
            if wait:
                self.hold, self.held = None, True
        if wait:
            self.release.wait()
        with self.lock:
            self.delivered.append(record)

    def hold_next(self, message):
        with self.lock:
            self.hold, self.release = message, threading.Event()

    def rebind(self):
        with self.lock:
            self.rebinds += 1
            if self.down:
                error = self.outage["rebind_error"]
                raise ResolutionError(error["status"], error["cause"])
            return self


def eventually(test, what, condition, timeout=10.0):
    limit = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > limit:
            test.fail("timed out waiting for " + what)
        time.sleep(0.001)


@unittest.skipUnless(FIXTURE.is_file(), "conformance/faults/sink-outage is not beside this package")
class SinkOutageFixtureTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def run_fixture(self, reporter):
        f = self.fixture
        options = f["options"]
        sink = OutageSink(f["outage"])
        transitions, failures = [], []
        callbacks = {}
        if not reporter:
            callbacks = dict(on_failure=lambda error, counts: (failures.append(error), transitions.append("failure")),
                             on_overflow=lambda counts: transitions.append("overflow"),
                             on_recovery=lambda counts: transitions.append("recovery"))
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            handler = ServiceHandler(sink, program=options["program"], capacity=options["capacity"],
                                     initial_backoff=options["initial_backoff_ms"] / 1000.0,
                                     max_backoff=options["max_backoff_ms"] / 1000.0, rebind=sink.rebind, **callbacks)
            self.addCleanup(handler.close, 1.0)
            log = logging.getLogger("sink-outage.%s" % reporter)
            log.propagate = False
            log.setLevel(logging.INFO)
            log.addHandler(handler)
            self.addCleanup(log.removeHandler, handler)
            reported = (lambda: len(stderr.getvalue().splitlines())) if reporter else (lambda: len(transitions))
            final = None
            for index, step in enumerate(f["steps"]):
                if "defaults" in step:
                    defaults=ServiceHandler(sink)
                    try:
                        self.assertEqual(dict(capacity=defaults._capacity,
                            initial_backoff_ms=round(defaults._initial_backoff*1000),
                            max_backoff_ms=round(defaults._max_backoff*1000),
                            write_timeout_ms=round(defaults._write_timeout*1000),
                            jitter_percent=round(BACKOFF_JITTER*100),gap_message=GAP_MESSAGE),step["defaults"])
                    finally:
                        defaults.close(1.0)
                elif "log" in step:
                    for message in step["log"]:
                        log.info(message)
                elif "outage" in step:
                    with sink.lock:
                        sink.down = step["outage"]
                elif "hold" in step:
                    sink.hold_next(step["hold"])
                elif step.get("release"):
                    sink.release.set()
                elif step.get("await") == "held":
                    eventually(self, "the held delivery", lambda: sink.held)
                elif "counts" in step:
                    self.assertEqual(self.counts(handler.counts()), step["counts"], "step %d" % index)
                elif step.get("close"):
                    final = handler.close(5.0)
                elif step.get("await") == "settled":
                    def settled():
                        c = handler.counts()
                        return c.queued == 0 and not c.failing and c.written + c.failed + c.abandoned == c.accepted
                    eventually(self, "settled", settled)
                elif step.get("await") == "failing":
                    eventually(self, "the failure report", lambda: handler.counts().failing and reported() >= 1)
                elif step.get("await") == "retry":
                    eventually(self, "a retry during the outage", lambda: sink.rebinds >= 2)
                else:
                    self.fail("step %d is unknown: %r" % (index, step))
        expect = f["expect"]
        self.assertEqual(self.counts(final), expect["counts"])
        self.assertIn(expect["last_error_contains"], final.last_error)
        self.assertEqual([r.msg for r in sink.delivered], expect["delivered"])
        gap, want = sink.delivered[2], expect["gap"]
        writer = gap.identity[0]
        self.assertEqual((gap.level, gap.msg, len(gap.identity), writer.hop, writer.by, writer.program),
                         (want["level"], want["msg"], 1, want["hop"], want["by"], want["program"]))
        for key, value in want["attrs"].items():
            self.assertEqual(gap.attrs[key], value)
        for key in want["timestamps"]:
            self.assertRegex(gap.attrs[key], INSTANT)
        self.assertLessEqual(gap.attrs["since"], gap.attrs["until"])
        # Every drop is reported by exactly one gap record; a later gap record
        # begins where the previous one ended.
        gaps = [r for r in sink.delivered if r.msg == want["msg"]]
        self.assertEqual([r.attrs["dropped"] for r in gaps], expect["gaps_dropped"])
        self.assertEqual(sum(int(r.attrs["dropped"]) for r in gaps), final.dropped)
        for previous, later in zip(gaps, gaps[1:]):
            self.assertEqual(later.attrs["since"], previous.attrs["until"])
        for r in gaps:
            for key in want["timestamps"]:
                self.assertRegex(r.attrs[key], INSTANT)
            self.assertLessEqual(r.attrs["since"], r.attrs["until"])
        if reporter:
            self.assertEqual(stderr.getvalue().splitlines(), expect["reporter"])
        else:
            self.assertEqual(stderr.getvalue(), "")
            self.assertEqual(transitions, expect["transitions"])
            self.assertEqual(failure_class(failures[0]), expect["failure_status"])

    @staticmethod
    def counts(c):
        return dict(accepted=c.accepted, dropped=c.dropped, written=c.written, failed=c.failed,
                    abandoned=c.abandoned, queued=c.queued, failing=c.failing)

    def test_callbacks(self):
        self.run_fixture(reporter=False)

    def test_default_reporter(self):
        self.run_fixture(reporter=True)


if __name__ == "__main__":
    unittest.main()
