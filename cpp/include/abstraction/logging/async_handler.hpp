#pragma once
// An asynchronous handler over a resolved logging client [LOG-S12, LOG-S13].
//
// write() queues a record and returns at once. One delivery thread hands the
// queued records to the writer in order. The queue holds capacity records
// (1024), counting the record being delivered, and a full queue drops the newest
// record and counts it. A failed delivery keeps its record at the head, rebinds
// at once, and retries after 100 ms, doubling up to 5 s with 20 % jitter,
// rebinding before each retry. Each delivery is bounded by write_timeout (5 s).
// A recovery first delivers one WARN gap record naming the records dropped since
// the last delivery. The first time the queue is empty after a recovery, another
// gap record names the records dropped after the first was built. Records go to
// no other sink.
//
// on_failure, on_recovery and on_overflow run once per transition on the thread
// that observed it. With none of them set, each transition is one line on
// std::cerr. The synchronous Logger::write in client.hpp is unchanged.
#include <abstraction/logging/client.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace abstraction::logging {

inline constexpr std::string_view kGapMessage = "records dropped while the sink was unreachable";

// A snapshot of an AsyncHandler. Every record handed to write() is exactly one
// of accepted or dropped; every accepted record ends as exactly one of written,
// failed or abandoned, or is still queued.
struct HandlerCounts {
    std::uint64_t accepted = 0;
    std::uint64_t dropped = 0;
    std::uint64_t written = 0;
    std::uint64_t failed = 0;
    std::uint64_t abandoned = 0;
    std::size_t queued = 0;
    bool failing = false;
    std::string last_error;
};

struct AsyncOptions {
    std::size_t capacity = 1024;
    // Zero or negative leaves deliveries unbounded.
    std::chrono::milliseconds write_timeout{5000};
    std::chrono::milliseconds initial_backoff{100};
    std::chrono::milliseconds max_backoff{5000};
    // Names the hop 0 claim of the gap record and of log().
    std::string program;
    std::function<void(std::exception_ptr, const HandlerCounts&)> on_failure;
    std::function<void(const HandlerCounts&)> on_recovery;
    std::function<void(const HandlerCounts&)> on_overflow;
};

// A delivery that did not return within write_timeout.
class WriteTimeout : public std::runtime_error {
public:
    explicit WriteTimeout(const std::string& message) : std::runtime_error(message) {}
};

namespace async_detail {

inline std::string message_of(const std::exception_ptr& error) {
    try {
        if (error) std::rethrow_exception(error);
    } catch (const std::exception& e) {
        return e.what();
    } catch (...) {
        return "unknown failure";
    }
    return "";
}

// The resolution status of the facade's ResolutionError, whose message is
// "service resolution: <status>[: ...]"; timeout; or write_failed.
inline std::string failure_class(const std::exception_ptr& error) {
    try {
        if (error) std::rethrow_exception(error);
    } catch (const WriteTimeout&) {
        return "timeout";
    } catch (const ipc::FrameError& e) {
        return e.status == ipc::Status::Timeout ? "timeout" : "write_failed";
    } catch (const std::exception& e) {
        const std::string_view prefix = "service resolution: ";
        std::string_view text = e.what();
        if (text.substr(0, prefix.size()) == prefix) {
            text.remove_prefix(prefix.size());
            return std::string(text.substr(0, text.find(':')));
        }
    } catch (...) {
    }
    return "write_failed";
}

inline std::string instant(std::chrono::system_clock::time_point at) {
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(at.time_since_epoch()).count();
    const std::time_t seconds = static_cast<std::time_t>(micros / 1000000);
    std::tm utc{};
#ifdef _WIN32
    gmtime_s(&utc, &seconds);
#else
    gmtime_r(&seconds, &utc);
#endif
    char date[32], text[48];
    std::strftime(date, sizeof(date), "%Y-%m-%dT%H:%M:%S", &utc);
    std::snprintf(text, sizeof(text), "%s.%06lldZ", date, static_cast<long long>(micros % 1000000));
    return text;
}

inline Attestation claim(const std::string& program) {
    Attestation self;
    self.by = "self";
    self.program = program;
    self.uid = self.gid = self.pid = -1;
    return self;
}

inline void report(const std::string& line) { std::cerr << line << '\n' << std::flush; }

struct Attempt {
    std::function<void(const Record&)> writer;
    Record record;
    std::mutex mutex;
    std::condition_variable done_signal;
    bool done = false;
    std::exception_ptr error;
};

// Calls the writer on a thread of its own, one record at a time, so the
// delivery thread can stop waiting for a delivery that exceeds its timeout. The
// thread owns this state; a hung writer is detached at shutdown.
struct BoundedWriter {
    std::mutex mutex;
    std::condition_variable ready;
    std::deque<std::shared_ptr<Attempt>> requests;
    bool stopping = false;

    static void run(std::shared_ptr<BoundedWriter> self) {
        for (;;) {
            std::shared_ptr<Attempt> attempt;
            {
                std::unique_lock<std::mutex> lock(self->mutex);
                self->ready.wait(lock, [&] { return self->stopping || !self->requests.empty(); });
                if (self->requests.empty()) return;
                attempt = self->requests.front();
                self->requests.pop_front();
            }
            std::exception_ptr error;
            try {
                attempt->writer(attempt->record);
            } catch (...) {
                error = std::current_exception();
            }
            {
                std::lock_guard<std::mutex> lock(attempt->mutex);
                attempt->error = error;
                attempt->done = true;
            }
            attempt->done_signal.notify_all();
        }
    }
};

}  // namespace async_detail

class AsyncHandler {
public:
    using Writer = std::function<void(const Record&)>;
    // Resolves the service again and returns the writer for the new binding;
    // throws the resolution error when it cannot.
    using Rebinder = std::function<Writer()>;

    explicit AsyncHandler(Writer writer, Rebinder rebind = {}, AsyncOptions options = {})
        : writer_(std::move(writer)), rebind_(std::move(rebind)), options_(std::move(options)) {
        if (!writer_) throw std::invalid_argument("logging: AsyncHandler needs a writer");
        if (options_.capacity == 0) options_.capacity = 1024;
        if (options_.initial_backoff.count() <= 0) options_.initial_backoff = std::chrono::milliseconds(100);
        if (options_.max_backoff.count() <= 0) options_.max_backoff = std::chrono::milliseconds(5000);
        if (!options_.on_failure && !options_.on_recovery && !options_.on_overflow) {
            options_.on_failure = [](std::exception_ptr error, const HandlerCounts& c) {
                async_detail::report("abstraction.logging: service unreachable (" + async_detail::failure_class(error) + "); " +
                                     std::to_string(c.queued) + " queued, " + std::to_string(c.dropped) + " dropped");
            };
            options_.on_overflow = [](const HandlerCounts& c) {
                async_detail::report("abstraction.logging: queue full, dropping newest records; " + std::to_string(c.queued) +
                                     " queued, " + std::to_string(c.dropped) + " dropped");
            };
            options_.on_recovery = [](const HandlerCounts& c) {
                async_detail::report("abstraction.logging: delivering again; " + std::to_string(c.dropped) + " dropped");
            };
        }
        if (options_.write_timeout.count() > 0) {
            bounded_ = std::make_shared<async_detail::BoundedWriter>();
            writer_thread_ = std::thread(&async_detail::BoundedWriter::run, bounded_);
        }
        delivery_ = std::thread([this] { run(); });
    }

    AsyncHandler(const AsyncHandler&) = delete;
    AsyncHandler& operator=(const AsyncHandler&) = delete;

    // Closes within write_timeout. Without a write timeout it waits for a
    // delivery in progress to return.
    ~AsyncHandler() {
        close(options_.write_timeout.count() > 0 ? options_.write_timeout : std::chrono::milliseconds(0));
        if (delivery_.joinable()) delivery_.join();
    }

    // Queues the record. Returns false when it was dropped or the handler is closed.
    bool write(Record record) {
        HandlerCounts snapshot;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) return false;
            if (queue_.size() < options_.capacity) {
                queue_.push_back(std::move(record));
                ++counts_.accepted;
                changed_.notify_all();
                return true;
            }
            ++counts_.dropped;
            ++lost_;
            if (overflowing_) return false;
            overflowing_ = true;
            snapshot = snapshot_locked();
        }
        if (options_.on_overflow) options_.on_overflow(snapshot);
        return false;
    }

    // Builds a record with the writer's hop 0 claim and queues it.
    bool log(std::int64_t level, std::string message, std::map<std::string, std::string> attributes = {}) {
        Record record;
        record.schema = 1;
        record.time = async_detail::instant(std::chrono::system_clock::now());
        record.level = level;
        record.msg = std::move(message);
        record.attrs = std::move(attributes);
        record.identity.push_back(async_detail::claim(options_.program));
        return write(std::move(record));
    }

    HandlerCounts counts() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return snapshot_locked();
    }

    // Waits until the queue is empty, the handler is failing, or timeout passes.
    void flush(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        changed_.wait_for(lock, timeout, [&] { return queue_.empty() || counts_.failing || stop_; });
    }

    // Stops accepting records and delivers the queue until timeout. At the
    // timeout the record being delivered is counted failed and the records
    // behind it abandoned. Without a write timeout, a delivery in progress keeps
    // running and its counts settle when it returns. Safe to call more than once.
    HandlerCounts close(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        closed_ = true;
        changed_.notify_all();
        if (delivery_.joinable()) {
            bool finished = changed_.wait_for(lock, timeout, [&] { return finished_; });
            if (!finished) {
                stop_ = true;
                changed_.notify_all();
                if (current_) {
                    std::lock_guard<std::mutex> attempt_lock(current_->mutex);
                    current_->done_signal.notify_all();
                }
                if (bounded_) changed_.wait(lock, [&] { return finished_; });
            }
            if (finished_) {
                lock.unlock();
                delivery_.join();
                lock.lock();
            }
        }
        if (bounded_ && writer_thread_.joinable()) {
            {
                std::lock_guard<std::mutex> writer_lock(bounded_->mutex);
                bounded_->stopping = true;
            }
            bounded_->ready.notify_all();
            if (pending_ || current_) writer_thread_.detach(); else writer_thread_.join();
        }
        return snapshot_locked();
    }

private:
    HandlerCounts snapshot_locked() const {
        HandlerCounts c = counts_;
        c.queued = queue_.size();
        return c;
    }

    void run() {
        for (;;) {
            if (std::exception_ptr drained = report_drained()) {
                if (stopped() || !retry(drained)) {
                    std::unique_lock<std::mutex> lock(mutex_);
                    counts_.abandoned += queue_.size();
                    queue_.clear();
                    return finish(lock);
                }
                continue;
            }
            Record record;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                changed_.wait(lock, [&] { return stop_ || !queue_.empty() || closed_; });
                if (stop_) {
                    counts_.abandoned += queue_.size();
                    queue_.clear();
                    return finish(lock);
                }
                if (queue_.empty()) return finish(lock);
                record = queue_.front();
            }
            std::exception_ptr error = deliver(record);
            if (!error) {
                std::unique_lock<std::mutex> lock(mutex_);
                pop();
                ++counts_.written;
                if (!draining_) lost_ = 0;
                changed_.notify_all();
                continue;
            }
            if (stopped() || !retry(error)) {
                std::unique_lock<std::mutex> lock(mutex_);
                if (!queue_.empty()) {
                    pop();
                    ++counts_.failed;
                }
                counts_.abandoned += queue_.size();
                queue_.clear();
                return finish(lock);
            }
        }
    }

    // The first time the queue is empty after a recovery, delivers a gap record
    // for the records dropped after the recovery's gap record was built
    // [LOG-S13]. Returns that delivery's error.
    std::exception_ptr report_drained() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!draining_ || !queue_.empty()) return nullptr;
            if (lost_ == 0) {
                draining_ = false;
                return nullptr;
            }
        }
        std::exception_ptr error;
        try {
            error = deliver(gap_record());
        } catch (...) {
            error = std::current_exception();
        }
        if (error) return error;
        std::lock_guard<std::mutex> lock(mutex_);
        lost_ -= reported_;
        since_ = until_;
        return nullptr;
    }

    void finish(std::unique_lock<std::mutex>& lock) {
        finished_ = true;
        changed_.notify_all();
        lock.unlock();
    }

    bool stopped() {
        std::lock_guard<std::mutex> lock(mutex_);
        return stop_;
    }

    void pop() {
        queue_.pop_front();
        if (queue_.empty()) overflowing_ = false;
    }

    std::exception_ptr deliver(const Record& record) {
        if (!bounded_) {
            try {
                writer_(record);
                return nullptr;
            } catch (...) {
                return std::current_exception();
            }
        }
        if (pending_) {
            if (!wait(pending_)) {
                return std::make_exception_ptr(WriteTimeout(
                    "abstraction.logging: delivery exceeded its write timeout: an earlier delivery has not returned"));
            }
            pending_.reset();
        }
        auto attempt = std::make_shared<async_detail::Attempt>();
        attempt->writer = writer_;
        attempt->record = record;
        {
            std::lock_guard<std::mutex> lock(bounded_->mutex);
            bounded_->requests.push_back(attempt);
        }
        bounded_->ready.notify_all();
        if (!wait(attempt)) {
            pending_ = attempt;
            return std::make_exception_ptr(WriteTimeout("abstraction.logging: delivery exceeded its write timeout (" +
                                                        std::to_string(options_.write_timeout.count()) + " ms)"));
        }
        return attempt->error;
    }

    // Waits for an attempt within write_timeout, or until close stops the
    // handler. close sets stop_ before it wakes current_, so a wait that starts
    // after the wake sees the flag.
    bool wait(const std::shared_ptr<async_detail::Attempt>& attempt) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stop_) return false;
            current_ = attempt;
        }
        bool done = false;
        {
            std::unique_lock<std::mutex> lock(attempt->mutex);
            attempt->done_signal.wait_for(lock, options_.write_timeout, [&] { return attempt->done || stop_.load(); });
            done = attempt->done;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        current_.reset();
        return done;
    }

    void rebind() {
        if (!rebind_) return;
        Writer next = rebind_();
        if (!next) throw std::runtime_error("logging: rebind returned no writer");
        writer_ = std::move(next);
    }

    bool retry(std::exception_ptr cause) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            counts_.failing = true;
            counts_.last_error = async_detail::message_of(cause);
            if (!draining_) since_ = std::chrono::system_clock::now();
            changed_.notify_all();
        }
        try {
            rebind();
        } catch (...) {
            cause = std::current_exception();
        }
        HandlerCounts snapshot;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            counts_.last_error = async_detail::message_of(cause);
            snapshot = snapshot_locked();
        }
        if (options_.on_failure) options_.on_failure(cause, snapshot);
        auto delay = options_.initial_backoff;
        std::uniform_real_distribution<double> jitter(0.8, 1.2);
        for (;;) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                const auto pause = std::chrono::duration_cast<std::chrono::microseconds>(delay * jitter(random_));
                if (changed_.wait_for(lock, pause, [&] { return stop_.load(); })) return false;
            }
            delay = std::min(delay * 2, options_.max_backoff);
            std::exception_ptr error;
            try {
                rebind();
                error = deliver(gap_record());
            } catch (...) {
                error = std::current_exception();
            }
            if (stopped()) return false;
            if (error) {
                std::lock_guard<std::mutex> lock(mutex_);
                counts_.last_error = async_detail::message_of(error);
                continue;
            }
            {
                std::lock_guard<std::mutex> lock(mutex_);
                counts_.failing = false;
                lost_ -= reported_;
                since_ = until_;
                draining_ = true;
                snapshot = snapshot_locked();
            }
            if (options_.on_recovery) options_.on_recovery(snapshot);
            return true;
        }
    }

    Record gap_record() {
        std::chrono::system_clock::time_point since;
        std::chrono::system_clock::time_point until;
        std::uint64_t lost;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            since = since_;
            lost = lost_;
            until = std::chrono::system_clock::now();
            reported_ = lost;
            until_ = until;
        }
        Record record;
        record.schema = 1;
        record.time = async_detail::instant(until);
        record.level = 4;
        record.msg = std::string(kGapMessage);
        record.identity.push_back(async_detail::claim(options_.program));
        record.attrs = {{"dropped", std::to_string(lost)},
                        {"since", async_detail::instant(since)},
                        {"until", async_detail::instant(until)}};
        return record;
    }

    Writer writer_;
    Rebinder rebind_;
    AsyncOptions options_;
    mutable std::mutex mutex_;
    std::condition_variable changed_;
    std::deque<Record> queue_;
    HandlerCounts counts_;
    bool closed_ = false;
    std::atomic<bool> stop_{false};
    bool finished_ = false;
    bool overflowing_ = false;
    std::uint64_t lost_ = 0;      // dropped records no delivered gap record reported
    std::uint64_t reported_ = 0;  // the dropped count the latest gap record carries
    bool draining_ = false;       // a recovery happened and the queue has not been empty since
    // The current failure began, or the previous gap record's until while draining.
    std::chrono::system_clock::time_point since_;
    std::chrono::system_clock::time_point until_;  // the until of the latest gap record built
    std::shared_ptr<async_detail::BoundedWriter> bounded_;
    std::shared_ptr<async_detail::Attempt> pending_;
    std::shared_ptr<async_detail::Attempt> current_;
    std::mt19937_64 random_{std::random_device{}()};
    std::thread writer_thread_;
    std::thread delivery_;
};

// An AsyncHandler over a resolved Logger. resolve binds again, for example
// [machine] { return machine.resolve_log({}, "local"); }, and throws the
// facade's resolution error when the runtime is gone.
inline std::unique_ptr<AsyncHandler> make_async_handler(Logger bound, std::function<Logger()> resolve = {},
                                                        AsyncOptions options = {}) {
    auto writer_for = [](Logger logger) -> AsyncHandler::Writer {
        return [logger](const Record& record) { logger.write(record); };
    };
    AsyncHandler::Rebinder rebind;
    if (resolve) rebind = [resolve, writer_for] { return writer_for(resolve()); };
    return std::make_unique<AsyncHandler>(writer_for(std::move(bound)), std::move(rebind), std::move(options));
}

}  // namespace abstraction::logging
