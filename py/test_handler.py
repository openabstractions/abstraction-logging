import contextlib
import io
import logging
import os
import socket
import threading
import time
import unittest

import abstraction.logging as rec
from abstraction.logging.handler import (DEFAULT_MAX_MESSAGE_BYTES, GAP_MESSAGE, HandlerCounts, ServiceHandler,
                                         claim, failure_class, resolved_handler, service_level, to_record)


class Sink:
    """Logs from inside every write, and raises while fail is set."""

    def __init__(self, logs=True):
        self.records = []
        self.fail = False
        self.logs = logs
        self.attempts = 0
        self.lock = threading.Lock()

    def write(self, record):
        self.attempts += 1
        if self.logs:
            logging.getLogger("fixture.transport").warning("transport logged during a write")
        if self.fail:
            raise ConnectionError("sink endpoint closed")
        with self.lock:
            self.records.append(record)

    def messages(self):
        with self.lock:
            return [r.msg for r in self.records]


class Hanging:
    """A sink whose write blocks until release is set."""

    def __init__(self):
        self.release = threading.Event()
        self.started = threading.Event()
        self.records = []

    def write(self, record):
        self.started.set()
        self.release.wait()
        self.records.append(record)


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def eventually(test, what, condition, timeout=10.0):
    limit = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > limit:
            test.fail("timed out waiting for " + what)
        time.sleep(0.001)


FAST = dict(initial_backoff=0.001, max_backoff=0.002)


class HandlerTests(unittest.TestCase):
    def setUp(self):
        transport = logging.getLogger("fixture.transport")
        self.addCleanup(lambda: [transport.removeHandler(h) for h in (self.handler, self.capture)])
        self.sink = Sink()
        self.events = []
        self.handler = self.make(self.sink, program="fixture-app",
                                 on_failure=lambda error, counts: self.events.append(("failure", repr(error), counts)),
                                 on_recovery=lambda counts: self.events.append(("recovery", counts)), **FAST)
        self.capture = Capture()
        self.log = logging.getLogger("fixture.app.%s" % self.id())
        self.log.propagate = False
        self.log.setLevel(logging.DEBUG)
        self.log.addHandler(self.handler)
        self.log.addHandler(self.capture)
        transport.addHandler(self.handler)
        transport.addHandler(self.capture)
        transport.propagate = False

    def make(self, sink, **options):
        handler = ServiceHandler(sink, **options)
        self.addCleanup(handler.close, 1.0)
        return handler

    def test_records_carry_contract_shape_and_the_writer_claim(self):
        self.log.debug("debug")
        self.log.info("model %s delivered in %d s", "a.safetensors", 3)
        self.log.log(25, "between info and warning")
        self.log.warning("warn")
        self.log.error("error")
        self.log.critical("critical")
        try:
            raise ValueError("bad archive")
        except ValueError:
            self.log.exception("install failed")
        self.handler.flush()
        messages = [r.msg for r in self.sink.records]
        self.assertEqual(messages, ["debug", "model a.safetensors delivered in 3 s", "between info and warning",
                                    "warn", "error", "critical", "install failed"])
        self.assertEqual([r.level for r in self.sink.records], [-4, 0, 2, 4, 8, 12, 8])
        for record in self.sink.records:
            self.assertEqual(record.schema, 1)
            self.assertRegex(record.time, r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$")
            self.assertEqual(record.attrs["logger"], self.log.name)
            self.assertRegex(record.attrs["source"], r"test_handler\.py:\d+$")
            self.assertEqual(len(record.identity), 1)
            writer = record.identity[0]
            self.assertEqual((writer.by, writer.verified, writer.hop, writer.program), ("self", False, 0, "fixture-app"))
            self.assertEqual((writer.uid, writer.gid, writer.pid, writer.host), (-1, -1, os.getpid(), socket.gethostname()))
            self.assertEqual(writer.exe, "")
            decoded = rec.decode(rec.encode(record))
            self.assertEqual((decoded.msg, decoded.level, decoded.identity[0].program), (record.msg, record.level, "fixture-app"))
        self.assertIn("ValueError: bad archive", self.sink.records[-1].attrs["exception"])
        self.assertEqual(self.handler.counts(), HandlerCounts(7, 0, 7, 0, 0, 0, False, ""))

    def test_transport_logging_inside_write_does_not_recurse(self):
        self.log.info("one")
        self.handler.flush()
        self.assertEqual([r.msg for r in self.sink.records], ["one"])
        self.assertIn("transport logged during a write", self.capture.messages)
        self.assertEqual(self.handler.counts().accepted, 1)

    def test_failure_is_retried_reported_once_and_recovery_delivers_the_gap_first(self):
        """[LOG-S12, LOG-S13] A failed delivery stays queued and is never counted failed."""
        self.sink.fail = True
        self.log.warning("kept one")
        self.log.warning("kept two")
        eventually(self, "the failure report", lambda: self.events)
        counts = self.handler.counts()
        self.assertEqual((counts.accepted, counts.written, counts.failed, counts.queued, counts.failing), (2, 0, 0, 2, True))
        self.assertIn("sink endpoint closed", counts.last_error)
        self.assertEqual(self.events[0][2].queued, 2)
        time.sleep(0.05)
        self.assertEqual([e[0] for e in self.events], ["failure"])
        self.sink.fail = False
        self.handler.flush()
        eventually(self, "the recovery", lambda: self.handler.counts().written == 2)
        self.assertEqual([e[0] for e in self.events], ["failure", "recovery"])
        self.assertEqual(self.sink.messages(), [GAP_MESSAGE, "kept one", "kept two"])
        gap = self.sink.records[0]
        self.assertEqual((gap.level, gap.attrs["dropped"], gap.identity[0].program), (4, "0", "fixture-app"))
        self.assertEqual(self.handler.counts(), HandlerCounts(2, 0, 2, 0, 0, 0, False, counts.last_error))

    def test_callbacks_that_log_reach_other_handlers_only(self):
        def noisy(error, counts):
            logging.getLogger(self.log.name).error("callback saw %s", error)
        handler = self.make(self.sink, on_failure=noisy, **FAST)
        self.log.removeHandler(self.handler)
        self.log.addHandler(handler)
        self.sink.fail = True
        self.log.warning("kept")
        eventually(self, "the callback", lambda: any("callback saw sink endpoint closed" in m for m in self.capture.messages))
        self.assertEqual(handler.counts().accepted, 1)
        self.log.removeHandler(handler)

    def test_threads_write_independently(self):
        threads = [threading.Thread(target=lambda n=n: [self.log.info("t%d-%d", n, i) for i in range(50)]) for n in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.handler.flush()
        self.assertEqual(len(self.sink.records), 200)
        self.assertEqual(self.handler.counts().written, 200)

    def test_emit_returns_at_once_while_the_sink_hangs(self):
        sink = Hanging()
        handler = self.make(sink, write_timeout=60.0)
        self.addCleanup(sink.release.set)
        log = logging.getLogger("fixture.hang")
        log.propagate = False
        log.addHandler(handler)
        self.addCleanup(log.removeHandler, handler)
        log.warning("held in flight")
        self.assertTrue(sink.started.wait(5))
        latencies = []
        for i in range(200):
            began = time.perf_counter()
            log.warning("while hung %d", i)
            latencies.append(time.perf_counter() - began)
        latencies.sort()
        self.assertLess(latencies[len(latencies) // 2], 0.001, "median emit %.6f s on a hung sink" % latencies[100])
        self.assertLess(latencies[int(len(latencies) * 0.95)], 0.001, "95th percentile emit on a hung sink: %r" % latencies[-10:])
        self.assertEqual(handler.counts().queued, 201)

    def test_a_burst_during_an_outage_drops_the_newest_and_the_gap_carries_the_count(self):
        self.sink = Sink(logs=False)
        self.sink.fail = True
        overflows = []
        handler = self.make(self.sink, program="burst", on_overflow=overflows.append, on_failure=lambda e, c: None, **FAST)
        log = logging.getLogger("fixture.burst")
        log.propagate = False
        log.addHandler(handler)
        self.addCleanup(log.removeHandler, handler)
        for i in range(2000):
            log.warning("b%04d", i)
        counts = handler.counts()
        self.assertEqual((counts.accepted, counts.dropped, counts.queued), (1024, 976, 1024))
        self.assertEqual(len(overflows), 1)
        attempts = self.sink.attempts
        eventually(self, "a retry after the burst", lambda: self.sink.attempts >= attempts + 2)
        self.sink.fail = False
        eventually(self, "the backlog", lambda: handler.counts().written == 1024)
        final = handler.close(5.0)
        self.assertEqual((final.written, final.failed, final.dropped, final.abandoned), (1024, 0, 976, 0))
        messages = self.sink.messages()
        self.assertEqual((len(messages), messages[0], messages[1], messages[-1]), (1025, GAP_MESSAGE, "b0000", "b1023"))
        self.assertEqual(self.sink.records[0].attrs["dropped"], "976")
        self.assertEqual(len(overflows), 1)

    def test_without_callbacks_an_outage_is_two_lines_on_stderr_and_nothing_on_the_root_logger(self):
        root = logging.getLogger()
        seen = Capture()
        root.addHandler(seen)
        self.addCleanup(root.removeHandler, seen)
        self.sink = Sink(logs=False)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            handler = ServiceHandler(self.sink, **FAST)
            log = logging.getLogger("fixture.quiet")
            log.propagate = False
            log.addHandler(handler)
            self.sink.fail = True
            for i in range(3):
                log.warning("secret record %d", i)
            eventually(self, "a retry", lambda: handler.counts().failing)
            time.sleep(0.02)
            self.sink.fail = False
            counts = handler.close(5.0)
            log.removeHandler(handler)
        self.assertEqual(stderr.getvalue().splitlines(), [
            "abstraction.logging: service unreachable (write_failed); 3 queued, 0 dropped",
            "abstraction.logging: delivering again; 0 dropped",
        ])
        self.assertEqual(seen.messages, [])
        self.assertEqual((counts.written, counts.failed), (3, 0))

    def test_close_at_its_timeout_during_an_outage_counts_the_rest_abandoned(self):
        self.sink = Sink(logs=False)
        self.sink.fail = True
        handler = self.make(self.sink, on_failure=lambda e, c: None, **FAST)
        log = logging.getLogger("fixture.close")
        log.propagate = False
        log.addHandler(handler)
        self.addCleanup(log.removeHandler, handler)
        for i in range(5):
            log.warning("a%d", i)
        eventually(self, "the failure", lambda: handler.counts().failing)
        began = time.monotonic()
        counts = handler.close(0.1)
        self.assertLess(time.monotonic() - began, 2.0)
        self.assertEqual((counts.accepted, counts.failed, counts.abandoned, counts.written, counts.queued), (5, 1, 4, 0, 0))

    def test_resolved_handler_rebinds_through_the_facade_factory(self):
        sinks, refusals = [Sink(), Sink()], []

        class ResolutionError(RuntimeError):
            status = "runtime_unavailable"

        class Machine:
            def resolve_log(self, *, scope):
                refusals.append(scope)
                if len(refusals) == 2:
                    raise ResolutionError("service resolution: runtime_unavailable")
                return sinks[0] if len(refusals) == 1 else sinks[1]

        failures = []
        handler = resolved_handler(Machine, program="rebinding", on_failure=lambda e, c: failures.append(e), **FAST)
        self.addCleanup(handler.close, 1.0)
        sinks[0].fail = True
        log = logging.getLogger("fixture.rebind")
        log.propagate = False
        log.addHandler(handler)
        self.addCleanup(log.removeHandler, handler)
        log.warning("moves to the new binding")
        eventually(self, "delivery through the new binding", lambda: "moves to the new binding" in sinks[1].messages())
        self.assertEqual(refusals[:3], ["local", "local", "local"])
        self.assertEqual(sinks[1].messages(), [GAP_MESSAGE, "moves to the new binding"])
        self.assertEqual(failure_class(failures[0]), "runtime_unavailable")

    def test_resolved_handler_raises_the_resolution_error_and_makes_no_handler(self):
        class NoRuntime:
            def resolve_log(self, *, scope):
                raise RuntimeError("service resolution: runtime_unavailable")
        before = threading.active_count()
        with self.assertRaises(RuntimeError):
            resolved_handler(NoRuntime)
        self.assertEqual(threading.active_count(), before)

    def test_bounds_and_encodable_text(self):
        record = logging.LogRecord("fixture", logging.INFO, __file__, 1, "\u00e9" * 20000, None, None)
        converted = to_record(record)
        self.assertLessEqual(len(converted.msg.encode("utf-8")), DEFAULT_MAX_MESSAGE_BYTES)
        self.assertEqual(converted.attrs["truncated"], "true")
        surrogate = logging.LogRecord("fixture", logging.INFO, __file__, 1, "bad \udc80 byte", None, None)
        rec.encode(to_record(surrogate))
        short = to_record(logging.LogRecord("fixture", logging.INFO, __file__, 1, "x" * 10, None, None), max_message_bytes=4)
        self.assertEqual((short.msg, short.attrs["truncated"]), ("xxxx", "true"))

    def test_level_scale(self):
        self.assertEqual([service_level(n) for n in (0, 5, 10, 15, 20, 25, 30, 40, 50, 55)], [-8, -6, -4, -2, 0, 2, 4, 8, 12, 14])
        with self.assertRaises(TypeError):
            service_level(True)

    def test_claim(self):
        writer = claim("p")
        self.assertEqual((writer.by, writer.verified, writer.hop, writer.uid, writer.gid), ("self", False, 0, -1, -1))

    def test_failure_class(self):
        class ResolutionError(RuntimeError):
            status = "not_ready"
        self.assertEqual([failure_class(e) for e in (ResolutionError("x"), TimeoutError(), ConnectionError())],
                         ["not_ready", "timeout", "write_failed"])

    def test_constructor_refuses_invalid_arguments(self):
        with self.assertRaises(TypeError):
            ServiceHandler(object())
        with self.assertRaises(TypeError):
            ServiceHandler(self.sink, program=3)
        for bad in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                ServiceHandler(self.sink, max_message_bytes=bad)
            with self.assertRaises(ValueError):
                ServiceHandler(self.sink, capacity=bad)
        with self.assertRaises(ValueError):
            ServiceHandler(self.sink, initial_backoff=0)
        with self.assertRaises(TypeError):
            ServiceHandler(self.sink, on_failure="warn")
        with self.assertRaises(TypeError):
            ServiceHandler(self.sink, rebind="again")


if __name__ == "__main__":
    unittest.main()
