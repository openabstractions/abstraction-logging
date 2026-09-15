import logging
import os
import re
import socket
import threading
import unittest

from abstraction.logging import rec
from abstraction.logging.handler import (DEFAULT_MAX_MESSAGE_BYTES, HandlerCounts, ServiceHandler, claim,
                                         service_level, to_record)


class Sink:
    def __init__(self):
        self.records = []
        self.fail = False

    def Write(self, record):
        logging.getLogger("fixture.transport").warning("transport logged during a write")
        if self.fail:
            raise ConnectionError("sink endpoint closed")
        self.records.append(record)


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.sink = Sink()
        self.events = []
        self.handler = ServiceHandler(self.sink, program="fixture-app",
                                      on_failure=lambda error, counts: self.events.append(("failure", repr(error), counts)),
                                      on_recovery=lambda counts: self.events.append(("recovery", counts)))
        self.capture = Capture()
        self.log = logging.getLogger("fixture.app.%s" % self.id())
        self.log.propagate = False
        self.log.setLevel(logging.DEBUG)
        self.log.addHandler(self.handler)
        self.log.addHandler(self.capture)
        transport = logging.getLogger("fixture.transport")
        transport.addHandler(self.handler)
        transport.addHandler(self.capture)
        transport.propagate = False
        self.addCleanup(lambda: [transport.removeHandler(h) for h in (self.handler, self.capture)])

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
        self.assertEqual(self.handler.counts(), HandlerCounts(7, 0, False, ""))

    def test_transport_logging_inside_write_does_not_recurse(self):
        self.log.info("one")
        self.assertEqual([r.msg for r in self.sink.records], ["one"])
        self.assertIn("transport logged during a write", self.capture.messages)

    def test_failure_is_counted_reported_once_and_recovery_reported(self):
        self.sink.fail = True
        self.log.warning("lost one")
        self.log.warning("lost two")
        counts = self.handler.counts()
        self.assertEqual((counts.written, counts.failed, counts.failing), (0, 2, True))
        self.assertIn("sink endpoint closed", counts.last_error)
        self.assertEqual([e[0] for e in self.events], ["failure"])
        self.assertEqual(self.events[0][2].failed, 1)
        self.sink.fail = False
        self.log.warning("delivered")
        self.assertEqual([e[0] for e in self.events], ["failure", "recovery"])
        self.assertEqual(self.handler.counts(), HandlerCounts(1, 2, False, counts.last_error))
        self.assertEqual([r.msg for r in self.sink.records], ["delivered"])

    def test_callbacks_that_log_reach_other_handlers_only(self):
        def noisy(error, counts):
            logging.getLogger(self.log.name).error("callback saw %s", error)
        handler = ServiceHandler(self.sink, on_failure=noisy)
        self.log.removeHandler(self.handler)
        self.log.addHandler(handler)
        self.sink.fail = True
        self.log.warning("lost")
        self.assertIn("callback saw sink endpoint closed", self.capture.messages)
        self.assertEqual(handler.counts().failed, 1)

    def test_threads_write_independently(self):
        threads = [threading.Thread(target=lambda n=n: [self.log.info("t%d-%d", n, i) for i in range(50)]) for n in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(self.sink.records), 200)
        self.assertEqual(self.handler.counts().written, 200)

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

    def test_constructor_refuses_invalid_arguments(self):
        with self.assertRaises(TypeError):
            ServiceHandler(object())
        with self.assertRaises(TypeError):
            ServiceHandler(self.sink, program=3)
        for bad in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                ServiceHandler(self.sink, max_message_bytes=bad)
        with self.assertRaises(TypeError):
            ServiceHandler(self.sink, on_failure="warn")


if __name__ == "__main__":
    unittest.main()
