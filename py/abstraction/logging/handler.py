"""A Python logging.Handler that writes to one abstraction.logging sink.

ServiceHandler(sink, program=...) converts every LogRecord it handles into the
generated Record and calls sink.Write. The sink is any object with a callable
Write(Record): normally the SinkClient that Machine.resolve_log returns.

Records follow CONTRACT.md:
- schema 1 [LOG-R2];
- levels on the slog scale, DEBUG -4, INFO 0, WARNING 4, ERROR 8, CRITICAL 12,
  with Python levels between landmarks kept between them [LOG-R3, LOG-R4];
- string attributes [LOG-R5];
- UTC RFC 3339 time with microseconds [LOG-R7];
- identity hop 0 is the writer's own claim: by "self", never verified, uid and
  gid -1 [LOG-I1, LOG-I2, LOG-I11]. The receiving service appends its own hop.

Attributes are "logger", "source" (path:line), "thread" when named, and
"exception" for exc_info. A message or exception over max_message_bytes is cut
at a character boundary and marked with "truncated" or "exception.truncated".

A record logged on a thread that is already inside this handler's Write goes to
the other handlers only. A failed Write is counted and lost; on_failure(error,
counts) is called on each transition into failure, on_recovery(counts) on the
first success after one. Both callbacks run inside that guard, so anything they
log reaches the other handlers only.
"""
import datetime
import logging
import os
import socket
import threading
from collections import namedtuple

from . import rec

DEFAULT_MAX_MESSAGE_BYTES = 16384
UNKNOWN = -1
SELF = "self"

HandlerCounts = namedtuple("HandlerCounts", "written failed failing last_error")
HandlerCounts.__doc__ = """written, failed: records delivered to and lost at the sink.
failing: the most recent Write failed. last_error: repr of the latest failure, or ""."""


def service_level(levelno):
    """Python level numbers onto the slog scale: 10 -> -4, 20 -> 0, 25 -> 2, 30 -> 4."""
    if isinstance(levelno, bool) or not isinstance(levelno, int):
        raise TypeError("level must be an int")
    return (levelno - logging.INFO) * 2 // 5


def timestamp(created):
    moment = datetime.datetime.fromtimestamp(created, datetime.timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def claim(program=""):
    """The writer's own attestation: hop 0, mechanism self, unverified."""
    return rec.Attestation(by=SELF, verified=False, hop=0, program=program, host=socket.gethostname(),
                           uid=UNKNOWN, gid=UNKNOWN, pid=os.getpid())


def _bounded(text, limit):
    data = text.encode("utf-8", "replace")
    if len(data) <= limit:
        return data.decode("utf-8"), False
    return data[:limit].decode("utf-8", "ignore"), True


def to_record(record, *, program="", max_message_bytes=DEFAULT_MAX_MESSAGE_BYTES):
    """One logging.LogRecord as a generated Record carrying the writer's claim."""
    try:
        message = record.getMessage()
    except Exception:
        message = str(record.msg)
    attrs = {"logger": str(record.name), "source": "%s:%d" % (record.pathname, record.lineno)}
    if record.threadName:
        attrs["thread"] = str(record.threadName)
    if record.exc_info:
        text, cut = _bounded(logging.Formatter().formatException(record.exc_info), max_message_bytes)
        attrs["exception"] = text
        if cut:
            attrs["exception.truncated"] = "true"
    message, cut = _bounded(message, max_message_bytes)
    if cut:
        attrs["truncated"] = "true"
    return rec.Record(schema=1, time=timestamp(record.created), level=service_level(record.levelno),
                      msg=message, identity=[claim(program)], attrs=attrs)


class ServiceHandler(logging.Handler):
    """Writes each handled LogRecord to one abstraction.logging sink."""

    def __init__(self, sink, *, program="", level=logging.NOTSET, max_message_bytes=DEFAULT_MAX_MESSAGE_BYTES,
                 on_failure=None, on_recovery=None):
        if not callable(getattr(sink, "Write", None)):
            raise TypeError("sink must provide Write(Record)")
        if not isinstance(program, str):
            raise TypeError("program must be a str")
        if isinstance(max_message_bytes, bool) or not isinstance(max_message_bytes, int) or max_message_bytes < 1:
            raise ValueError("max_message_bytes must be a positive int")
        for name, callback in (("on_failure", on_failure), ("on_recovery", on_recovery)):
            if callback is not None and not callable(callback):
                raise TypeError(name + " must be callable")
        super().__init__(level)
        self._sink = sink
        self._program = program
        self._limit = max_message_bytes
        self._on_failure = on_failure
        self._on_recovery = on_recovery
        self._local = threading.local()
        self._counts_lock = threading.Lock()
        self._counts = HandlerCounts(0, 0, False, "")

    def counts(self):
        with self._counts_lock:
            return self._counts

    def _update(self, **changes):
        with self._counts_lock:
            previous = self._counts
            self._counts = previous._replace(**changes)
            return previous, self._counts

    def emit(self, record):
        if getattr(self._local, "active", False):
            return
        self._local.active = True
        try:
            try:
                converted = to_record(record, program=self._program, max_message_bytes=self._limit)
                self._sink.Write(converted)
            except Exception as error:
                with self._counts_lock:
                    previous = self._counts
                    self._counts = previous._replace(failed=previous.failed + 1, failing=True, last_error=repr(error))
                    current = self._counts
                if not previous.failing and self._on_failure is not None:
                    self._on_failure(error, current)
            else:
                with self._counts_lock:
                    previous = self._counts
                    self._counts = previous._replace(written=previous.written + 1, failing=False)
                    current = self._counts
                if previous.failing and self._on_recovery is not None:
                    self._on_recovery(current)
        finally:
            self._local.active = False
