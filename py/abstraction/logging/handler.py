"""A Python logging.Handler that delivers to one abstraction.logging sink.

ServiceHandler(sink, program=...) converts every LogRecord it handles into the
generated Record on the logging thread and queues it. One delivery thread calls
sink.write for each queued record in order, so emit never waits on the service.
The sink is any object with a callable write(Record): normally the SinkClient
that Machine.resolve_log returns. resolved_handler() resolves that client now
and rebinds through the same resolution when the service disappears.

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

A bound sink whose service disappears [LOG-S12]: the queue holds capacity
records (1024), counting the one being delivered, and a full queue drops the
newest record and counts it. A failed delivery keeps its record at the head,
rebinds at once, then retries after 100 ms, doubling up to 5 s with 20 % jitter,
rebinding before each retry. Each delivery is bounded by write_timeout (5 s).
Records go to no other sink.

The loss is visible [LOG-S13]: counts() returns accepted, dropped, written,
failed, abandoned, queued, failing and last_error. on_failure(error, counts),
on_recovery(counts) and on_overflow(counts) run once per transition; with none
of them, each transition is one line on sys.stderr. A recovery first delivers a
WARN gap record naming the records dropped since the last delivery. The first
time the queue is empty after a recovery, another gap record names the records
dropped after the first was built.

A record logged on the delivery thread, or from a transition callback, goes to
the other handlers only.
"""
import collections
import datetime
import logging
import os
import random
import socket
import sys
import threading
import time
from collections import namedtuple

import abstraction.logging as rec

DEFAULT_MAX_MESSAGE_BYTES = 16384
DEFAULT_CAPACITY = rec.DEFAULT_ASYNC_CAPACITY
DEFAULT_WRITE_TIMEOUT = rec.DEFAULT_WRITE_TIMEOUT_MS / 1000.0
DEFAULT_INITIAL_BACKOFF = rec.DEFAULT_INITIAL_BACKOFF_MS / 1000.0
DEFAULT_MAX_BACKOFF = rec.DEFAULT_MAX_BACKOFF_MS / 1000.0
DEFAULT_CLOSE_TIMEOUT = 5.0
BACKOFF_JITTER = rec.WRITER_BACKOFF_JITTER_PERCENT / 100.0
GAP_MESSAGE = rec.WRITER_GAP_MESSAGE
UNKNOWN = -1
SELF = "self"
WARN = 4

HandlerCounts = namedtuple("HandlerCounts", "accepted dropped written failed abandoned queued failing last_error")
HandlerCounts.__doc__ = """A snapshot of a ServiceHandler [LOG-S13].

Every handled record is exactly one of accepted or dropped. Every accepted record
ends as exactly one of written, failed (the record being delivered when close
reached its timeout) or abandoned (behind it), or is still queued. failing: the
latest delivery failed and the handler is retrying. last_error: repr of the
latest failure, or ""."""


class DeliveryTimeout(TimeoutError):
    """A delivery did not return within write_timeout."""


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


def failure_class(error):
    """The default reporter's name for a failure: the resolution status, timeout or write_failed."""
    status = getattr(error, "status", None)
    if type(error).__name__ == "ResolutionError" and isinstance(status, str):
        return status
    if isinstance(error, TimeoutError):
        return "timeout"
    return "write_failed"


def _report(line):
    stream = sys.stderr
    if stream is not None:
        stream.write(line + "\n")
        stream.flush()


def report_failure(error, counts):
    _report("abstraction.logging: service unreachable (%s); %d queued, %d dropped"
            % (failure_class(error), counts.queued, counts.dropped))


def report_overflow(counts):
    _report("abstraction.logging: queue full, dropping newest records; %d queued, %d dropped"
            % (counts.queued, counts.dropped))


def report_recovery(counts):
    _report("abstraction.logging: delivering again; %d dropped" % counts.dropped)


def _jittered(delay):
    return delay * (1 - BACKOFF_JITTER + 2 * BACKOFF_JITTER * random.random())


def _positive(name, value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
        raise ValueError(name + " must be a positive number")


class _Attempt:
    __slots__ = ("sink", "record", "done", "error", "event")

    def __init__(self, sink, record):
        self.sink, self.record, self.done, self.error = sink, record, False, None
        self.event = threading.Event()


class _BoundedWriter:
    """Calls sink.write on a thread of its own, one record at a time, so the
    delivery thread can stop waiting for a delivery that exceeds its timeout."""

    def __init__(self, owner):
        self._owner = owner
        self._requests = collections.deque()
        self._ready = threading.Condition()
        self._pending = None
        self._current = None
        self.thread = threading.Thread(target=self._loop, name="abstraction.logging writer", daemon=True)
        self.thread.start()

    def _loop(self):
        while True:
            with self._ready:
                while not self._requests:
                    self._ready.wait()
                attempt = self._requests.popleft()
            if attempt is None:
                return
            try:
                attempt.sink.write(attempt.record)
            except BaseException as error:
                # A delivery failure of any kind goes back to the delivery thread.
                attempt.error = error
            attempt.done = True
            attempt.event.set()

    def _wait(self, attempt, timeout):
        # close() sets the stop flag before it wakes the current wait, so a wait
        # that starts after the wake sees the flag.
        self._current = attempt
        try:
            if not self._owner._stop.is_set():
                attempt.event.wait(timeout)
        finally:
            self._current = None
        return attempt.done

    def deliver(self, sink, record, timeout):
        if self._pending is not None:
            if not self._wait(self._pending, timeout):
                raise DeliveryTimeout("abstraction.logging: delivery exceeded its write timeout: "
                                      "an earlier delivery has not returned")
            self._pending = None
        attempt = _Attempt(sink, record)
        with self._ready:
            self._requests.append(attempt)
            self._ready.notify()
        if not self._wait(attempt, timeout):
            self._pending = attempt
            raise DeliveryTimeout("abstraction.logging: delivery exceeded its write timeout (%gs)" % timeout)
        if attempt.error is not None:
            raise attempt.error

    def wake(self):
        current = self._current
        if current is not None:
            current.event.set()

    def stop(self):
        with self._ready:
            self._requests.append(None)
            self._ready.notify()


class ServiceHandler(logging.Handler):
    """Queues each handled LogRecord and delivers it to one abstraction.logging sink."""

    def __init__(self, sink, *, program="", level=logging.NOTSET, max_message_bytes=DEFAULT_MAX_MESSAGE_BYTES,
                 capacity=DEFAULT_CAPACITY, write_timeout=DEFAULT_WRITE_TIMEOUT,
                 initial_backoff=DEFAULT_INITIAL_BACKOFF, max_backoff=DEFAULT_MAX_BACKOFF, rebind=None,
                 on_failure=None, on_recovery=None, on_overflow=None):
        if not callable(getattr(sink, "write", None)):
            raise TypeError("sink must provide write(Record)")
        if not isinstance(program, str):
            raise TypeError("program must be a str")
        if isinstance(max_message_bytes, bool) or not isinstance(max_message_bytes, int) or max_message_bytes < 1:
            raise ValueError("max_message_bytes must be a positive int")
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("capacity must be a positive int")
        if write_timeout is not None:
            _positive("write_timeout", write_timeout)
        _positive("initial_backoff", initial_backoff)
        _positive("max_backoff", max_backoff)
        for name, callback in (("rebind", rebind), ("on_failure", on_failure), ("on_recovery", on_recovery),
                               ("on_overflow", on_overflow)):
            if callback is not None and not callable(callback):
                raise TypeError(name + " must be callable")
        super().__init__(level)
        if on_failure is None and on_recovery is None and on_overflow is None:
            on_failure, on_recovery, on_overflow = report_failure, report_recovery, report_overflow
        self._sink = sink
        self._program = program
        self._limit = max_message_bytes
        self._capacity = capacity
        self._write_timeout = write_timeout
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._rebind = rebind
        self._on_failure = on_failure
        self._on_recovery = on_recovery
        self._on_overflow = on_overflow
        self._local = threading.local()
        self._cond = threading.Condition()
        self._queue = collections.deque()
        self._accepted = self._dropped = self._written = self._failed = self._abandoned = 0
        self._failing = False
        self._last_error = ""
        self._overflowing = False
        self._since = 0.0  # the current failure began, or the previous gap record's until while draining
        self._until = 0.0  # the until of the latest gap record built
        self._lost = 0  # dropped records no delivered gap record reported
        self._reported = 0  # the dropped count the latest gap record carries
        self._draining = False  # a recovery happened and the queue has not been empty since
        self._closed = False
        self._stop = threading.Event()
        self._writer = _BoundedWriter(self) if write_timeout is not None else None
        self._thread = threading.Thread(target=self._run, name="abstraction.logging delivery", daemon=True)
        self._own = {self._writer.thread.ident} if self._writer is not None else set()
        self._thread.start()
        self._own.add(self._thread.ident)

    def counts(self):
        with self._cond:
            return self._snapshot()

    def _snapshot(self):
        return HandlerCounts(self._accepted, self._dropped, self._written, self._failed, self._abandoned,
                             len(self._queue), self._failing, self._last_error)

    def handle(self, record):
        # The delivery threads never enter the handler lock: logging.shutdown
        # holds it while close() waits for them.
        if threading.get_ident() in self._own:
            return False
        return super().handle(record)

    def emit(self, record):
        if getattr(self._local, "active", False):
            return
        self._local.active = True
        try:
            try:
                converted = to_record(record, program=self._program, max_message_bytes=self._limit)
            except Exception:
                self.handleError(record)
                return
            with self._cond:
                if self._closed:
                    return
                if len(self._queue) < self._capacity:
                    self._queue.append(converted)
                    self._accepted += 1
                    self._cond.notify_all()
                    return
                self._dropped += 1
                self._lost += 1
                first = not self._overflowing
                self._overflowing = True
                snapshot = self._snapshot()
            if first and self._on_overflow is not None:
                self._on_overflow(snapshot)
        finally:
            self._local.active = False

    def flush(self, timeout=None):
        """Wait until the queue is empty, the handler is failing, or timeout (write_timeout) passes."""
        limit = time.monotonic() + (timeout if timeout is not None else (self._write_timeout or DEFAULT_WRITE_TIMEOUT))
        with self._cond:
            while self._queue and not self._failing and not self._stop.is_set():
                remaining = limit - time.monotonic()
                if remaining <= 0:
                    return
                self._cond.wait(remaining)

    def close(self, timeout=DEFAULT_CLOSE_TIMEOUT):
        """Stop accepting records and deliver the queue for up to timeout seconds.

        At the timeout the record being delivered is counted failed and the
        records behind it abandoned. Returns the counts."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        if self._thread.ident != threading.get_ident():
            self._thread.join(timeout)
            if self._thread.is_alive():
                self._stop.set()
                with self._cond:
                    self._cond.notify_all()
                if self._writer is not None:
                    self._writer.wake()
                    self._thread.join()
        if self._writer is not None and not self._thread.is_alive():
            self._writer.stop()
        super().close()
        return self.counts()

    def _run(self):
        while True:
            try:
                self._report_drained()
            except Exception as error:
                if self._stop.is_set() or not self._retry(error):
                    self._give_up()
                    return
                continue
            record = self._next()
            if record is None:
                return
            try:
                self._deliver(record)
            except Exception as error:
                if self._stop.is_set() or not self._retry(error):
                    self._give_up()
                    return
            else:
                with self._cond:
                    self._pop()
                    self._written += 1
                    if not self._draining:
                        self._lost = 0
                    self._cond.notify_all()

    def _report_drained(self):
        """The first time the queue is empty after a recovery, deliver a gap record
        for the records dropped after the recovery's gap record was built [LOG-S13]."""
        with self._cond:
            if not self._draining or self._queue:
                return
            if self._lost == 0:
                self._draining = False
                return
        self._deliver(self._gap_record())
        with self._cond:
            self._lost -= self._reported
            self._since = self._until

    def _next(self):
        with self._cond:
            while True:
                if self._stop.is_set():
                    self._abandoned += len(self._queue)
                    self._queue.clear()
                    self._cond.notify_all()
                    return None
                if self._queue:
                    return self._queue[0]
                if self._closed:
                    return None
                self._cond.wait()

    def _deliver(self, record):
        if self._writer is None:
            self._sink.write(record)
        else:
            self._writer.deliver(self._sink, record, self._write_timeout)

    def _rebind_once(self):
        if self._rebind is None:
            return
        sink = self._rebind()
        if not callable(getattr(sink, "write", None)):
            raise TypeError("rebind must return a sink with write(Record)")
        self._sink = sink

    def _retry(self, cause):
        with self._cond:
            self._failing = True
            self._last_error = repr(cause)
            if not self._draining:
                self._since = time.time()
            self._cond.notify_all()
        try:
            self._rebind_once()
        except Exception as error:
            cause = error
        with self._cond:
            self._last_error = repr(cause)
            snapshot = self._snapshot()
        if self._on_failure is not None:
            self._on_failure(cause, snapshot)
        delay = self._initial_backoff
        while True:
            if self._stop.wait(_jittered(delay)):
                return False
            delay = min(2 * delay, self._max_backoff)
            try:
                self._rebind_once()
                self._deliver(self._gap_record())
            except Exception as error:
                if self._stop.is_set():
                    return False
                with self._cond:
                    self._last_error = repr(error)
                continue
            if self._stop.is_set():
                return False
            with self._cond:
                self._failing = False
                self._lost -= self._reported
                self._since = self._until
                self._draining = True
                snapshot = self._snapshot()
            if self._on_recovery is not None:
                self._on_recovery(snapshot)
            return True

    def _gap_record(self):
        with self._cond:
            since, lost = self._since, self._lost
            until = time.time()
            self._reported, self._until = lost, until
        return rec.Record(schema=1, time=timestamp(until), level=WARN, msg=GAP_MESSAGE, identity=[claim(self._program)],
                          attrs={"dropped": str(lost), "since": timestamp(since), "until": timestamp(until)})

    def _give_up(self):
        with self._cond:
            if self._queue:
                self._pop()
                self._failed += 1
            self._abandoned += len(self._queue)
            self._queue.clear()
            self._cond.notify_all()

    def _pop(self):
        self._queue.popleft()
        if not self._queue:
            self._overflowing = False


def resolved_handler(machine_factory=None, *, program="", scope=None, **options):
    """Resolve abstraction.logging/sink@1 now and return a ServiceHandler that
    rebinds through the same resolution [LOG-S11, LOG-S12].

    machine_factory returns an object with resolve_log(); the default is
    abstraction.facade.client.Machine, whose default discovery selects the
    installed runtime again at each rebind. scope is an
    abstraction.facade.Scope and defaults to Scope.LOCAL. The resolution error
    of an absent service is raised here, and no handler is made."""
    if machine_factory is None:
        from abstraction.facade.client import Machine as machine_factory
    if scope is None:
        from abstraction.facade import Scope
        scope = Scope.LOCAL

    def resolve():
        return machine_factory().resolve_log(scope=scope)

    return ServiceHandler(resolve(), program=program, rebind=resolve, **options)
