"""The native logger fallback of resolved_handler [LOG-S15].

When the machine answers that it offers no logging service, records go to the
stdlib logging.Handler the adopter installed beside the facade. Every other
resolution failure stays the resolution error of LOG-S11.
"""
import io
import logging
import os
import threading
import time
import unittest
from unittest import mock

import abstraction.logging.handler as handler_module
from abstraction.logging.handler import resolved_handler, not_offered

FAST = dict(initial_backoff=0.001, max_backoff=0.002)


class Sink:
    def __init__(self):
        self.records = []
        self.lock = threading.Lock()

    def write(self, record):
        with self.lock:
            self.records.append(record)

    def messages(self):
        with self.lock:
            return [r.msg for r in self.records]


class Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []
        self._mu = threading.Lock()

    def emit(self, record):
        with self._mu:
            self.records.append(record)

    def lines(self):
        with self._mu:
            return [(r.levelname, r.name, r.getMessage()) for r in self.records]


def eventually(test, what, condition, timeout=10.0):
    limit = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > limit:
            test.fail("timed out waiting for " + what)
        time.sleep(0.001)


class ResolutionError(RuntimeError):
    """The facade's resolution error, in the shape not_offered reads."""

    def __init__(self, status, looked_for="the explicit endpoint fixture"):
        super().__init__("service resolution: " + status)
        self.status, self.looked_for, self.platform = status, looked_for, None


def logger(test, name, handler):
    log = logging.getLogger(name)
    log.propagate = False
    log.setLevel(logging.DEBUG)
    log.addHandler(handler)
    test.addCleanup(log.removeHandler, handler)
    return log


class NativeFallbackTests(unittest.TestCase):
    def test_no_installation_sends_records_to_native_with_the_real_library(self):
        # The real abstraction.ipc.Library with no bundled native client, no
        # configuration and no recorded installation: OA is not installed.
        from abstraction.facade.client import Machine
        from abstraction.ipc import _native
        environment = {k: v for k, v in os.environ.items()
                       if k not in ("ABSTRACTION_IPC_LIBRARY", "ABSTRACTION_IPC_PREFIX", "ABSTRACTION_RUNTIME_ENDPOINT")}
        native = Capture()
        with mock.patch.dict(os.environ, environment, clear=True), \
                mock.patch.object(_native, "bundled_library_path", return_value=None), \
                mock.patch.object(_native, "registry_reader", None):
            with self.assertRaises(ValueError) as absent:
                Machine()
            self.assertTrue(not_offered(absent.exception))
            handler = resolved_handler(program="my-app", native=native, **FAST)
        self.addCleanup(handler.close, 1.0)
        log = logger(self, "my-app.worker", handler)
        log.info("worker started")
        try:
            raise KeyError("missing")
        except KeyError:
            log.exception("job failed")
        counts = handler.close(5.0)
        self.assertEqual((counts.native, counts.written, counts.failed, counts.abandoned), (2, 0, 0, 0))
        lines = native.lines()
        self.assertEqual(lines[0], ("WARNING", "abstraction.logging",
                                    "abstraction.logging/sink@1 not offered: runtime_unavailable"))
        self.assertEqual(lines[1:], [("INFO", "my-app.worker", "worker started"),
                                     ("ERROR", "my-app.worker", "job failed")])
        text = logging.Formatter().format(native.records[2])
        self.assertIn("KeyError: 'missing'", text)
        self.assertTrue(native.records[1].pathname.endswith("test_native_fallback.py"))
        self.assertGreater(native.records[1].lineno, 0)

    def test_the_service_takes_over_when_a_recheck_finds_it(self):
        saved = handler_module.NATIVE_RECHECK_INTERVAL
        handler_module.NATIVE_RECHECK_INTERVAL = 0.05
        self.addCleanup(setattr, handler_module, "NATIVE_RECHECK_INTERVAL", saved)
        sink, offered = Sink(), threading.Event()

        class Machine:
            def resolve_log(self, *, scope):
                if not offered.is_set():
                    raise ResolutionError("unavailable")
                return sink

        native = Capture()
        handler = resolved_handler(Machine, program="handover", native=native, **FAST)
        self.addCleanup(handler.close, 1.0)
        log = logger(self, "handover", handler)
        log.warning("before the service")
        eventually(self, "the native record", lambda: handler.counts().native == 1)
        offered.set()
        time.sleep(0.1)
        log.warning("after the service")
        eventually(self, "the service record", lambda: sink.messages() == ["after the service"])
        counts = handler.close(5.0)
        self.assertEqual((counts.native, counts.written), (1, 1))
        self.assertEqual([m for _, _, m in native.lines()], [
            "abstraction.logging/sink@1 not offered: unavailable",
            "before the service",
            "abstraction.logging/sink@1 offered again; records go to the service",
        ])

    def test_other_resolution_errors_are_still_raised(self):
        class Stopped:
            def resolve_log(self, *, scope):
                raise ResolutionError("runtime_unavailable", "the installed runtime at fixture")

        before = threading.active_count()
        with mock.patch.object(handler_module.sys, "platform", "linux"):
            with self.assertRaises(ResolutionError):
                resolved_handler(Stopped, native=Capture())
        self.assertEqual(threading.active_count(), before)
        for refusal in ("forbidden", "not_ready", "incompatible"):
            self.assertFalse(not_offered(ResolutionError(refusal)))

    def test_without_native_the_resolution_error_is_raised(self):
        class Unavailable:
            def resolve_log(self, *, scope):
                raise ResolutionError("unavailable")

        with self.assertRaises(ResolutionError):
            resolved_handler(Unavailable)

    def test_native_must_be_a_logging_handler(self):
        with self.assertRaises(TypeError):
            resolved_handler(object, native=io.StringIO())


if __name__ == "__main__":
    unittest.main()
