// AsyncHandler under a sink outage [LOG-S12, LOG-S13], including the shared
// conformance/faults/sink-outage fixture every language runs.
// Run: sink_outage <repository>/conformance/faults/sink-outage/sink-outage.json
#include "abstraction/logging/async_handler.hpp"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace logging = abstraction::logging;
using namespace std::chrono_literals;

// An adopter fixture may route these same cases through its actual metadata
// adapter. The default remains the shared handler itself.
#ifndef OA_LOGGING_OUTAGE_HANDLER
#define OA_LOGGING_OUTAGE_HANDLER logging::AsyncHandler
#endif

namespace {

// A JSON value, enough to read the fixture.
struct Json {
    enum class Kind { Null, Bool, Number, String, Array, Object } kind = Kind::Null;
    bool boolean = false;
    double number = 0;
    std::string text;
    std::vector<Json> items;
    std::vector<std::pair<std::string, Json>> members;

    const Json* find(const std::string& key) const {
        for (const auto& member : members)
            if (member.first == key) return &member.second;
        return nullptr;
    }
    const Json& at(const std::string& key) const {
        if (const Json* value = find(key)) return *value;
        throw std::runtime_error("fixture has no member " + key);
    }
};

class JsonReader {
public:
    explicit JsonReader(std::string text) : text_(std::move(text)) {}
    Json read() {
        Json value = parse();
        space();
        if (pos_ != text_.size()) throw std::runtime_error("trailing bytes in fixture");
        return value;
    }

private:
    void space() {
        while (pos_ < text_.size() && std::isspace(static_cast<unsigned char>(text_[pos_]))) ++pos_;
    }
    char next() {
        space();
        if (pos_ >= text_.size()) throw std::runtime_error("fixture ends early");
        return text_[pos_];
    }
    void expect(char c) {
        if (next() != c) throw std::runtime_error(std::string("fixture expected ") + c);
        ++pos_;
    }
    std::string string() {
        expect('"');
        std::string out;
        while (text_.at(pos_) != '"') {
            if (text_[pos_] == '\\') ++pos_;
            out += text_.at(pos_++);
        }
        ++pos_;
        return out;
    }
    Json parse() {
        Json value;
        char c = next();
        if (c == '{') {
            value.kind = Json::Kind::Object;
            ++pos_;
            if (next() == '}') { ++pos_; return value; }
            for (;;) {
                std::string key = string();
                expect(':');
                value.members.emplace_back(std::move(key), parse());
                if (next() == ',') { ++pos_; continue; }
                expect('}');
                return value;
            }
        }
        if (c == '[') {
            value.kind = Json::Kind::Array;
            ++pos_;
            if (next() == ']') { ++pos_; return value; }
            for (;;) {
                value.items.push_back(parse());
                if (next() == ',') { ++pos_; continue; }
                expect(']');
                return value;
            }
        }
        if (c == '"') {
            value.kind = Json::Kind::String;
            value.text = string();
            return value;
        }
        if (text_.compare(pos_, 4, "true") == 0) { pos_ += 4; value.kind = Json::Kind::Bool; value.boolean = true; return value; }
        if (text_.compare(pos_, 5, "false") == 0) { pos_ += 5; value.kind = Json::Kind::Bool; return value; }
        if (text_.compare(pos_, 4, "null") == 0) { pos_ += 4; return value; }
        std::size_t used = 0;
        value.kind = Json::Kind::Number;
        value.number = std::stod(text_.substr(pos_), &used);
        pos_ += used;
        return value;
    }
    std::string text_;
    std::size_t pos_ = 0;
};

// The fake service: while down, writes fail and rebinds throw the facade's
// resolution error shape. A held message's next successful write waits until
// release.
struct Outage {
    std::mutex mutex;
    std::condition_variable released;
    bool down = false;
    std::vector<logging::Record> delivered;
    int rebinds = 0;
    int attempts = 0;
    std::string hold;
    bool held = false;
    bool release = false;
    std::string write_error = "sink-outage: write refused";
    std::string status = "runtime_unavailable";
    std::string cause = "sink-outage: rebind refused";

    logging::AsyncHandler::Writer writer(std::shared_ptr<Outage> self) {
        return [self](const logging::Record& record) {
            std::unique_lock<std::mutex> lock(self->mutex);
            ++self->attempts;
            if (self->down) throw std::runtime_error(self->write_error);
            if (!self->hold.empty() && record.msg == self->hold) {
                self->hold.clear();
                self->held = true;
                self->released.wait(lock, [&] { return self->release; });
            }
            self->delivered.push_back(record);
        };
    }
    void hold_next(const std::string& message) {
        std::lock_guard<std::mutex> lock(mutex);
        hold = message;
        release = false;
    }
    bool is_held() {
        std::lock_guard<std::mutex> lock(mutex);
        return held;
    }
    void release_held() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            release = true;
        }
        released.notify_all();
    }
    logging::AsyncHandler::Rebinder rebinder(std::shared_ptr<Outage> self) {
        return [self]() -> logging::AsyncHandler::Writer {
            {
                std::lock_guard<std::mutex> lock(self->mutex);
                ++self->rebinds;
                if (self->down)
                    throw std::runtime_error("service resolution: " + self->status +
                                             ": abstraction.logging/sink@1 (capability abstraction.logging) at the installed runtime: " +
                                             self->cause);
            }
            return self->writer(self);
        };
    }
    void set(bool value) {
        std::lock_guard<std::mutex> lock(mutex);
        down = value;
    }
    std::vector<logging::Record> records() {
        std::lock_guard<std::mutex> lock(mutex);
        return delivered;
    }
    int rebind_count() {
        std::lock_guard<std::mutex> lock(mutex);
        return rebinds;
    }
};

// Captures std::cerr for the lifetime of the object.
class CerrCapture {
public:
    CerrCapture() : saved_(std::cerr.rdbuf(buffer_.rdbuf())) {}
    ~CerrCapture() { std::cerr.rdbuf(saved_); }
    std::vector<std::string> lines() const {
        std::vector<std::string> out;
        std::istringstream in(buffer_.str());
        for (std::string line; std::getline(in, line);) out.push_back(line);
        return out;
    }

private:
    std::stringstream buffer_;
    std::streambuf* saved_;
};

int failures = 0;

void check(bool ok, const std::string& what) {
    if (!ok) {
        std::cout << "FAIL: " << what << "\n";
        ++failures;
    }
}

template <class Condition>
bool eventually(Condition condition, std::chrono::milliseconds limit = 10s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (!condition()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

std::string counts_text(const logging::HandlerCounts& c) {
    return "accepted=" + std::to_string(c.accepted) + " dropped=" + std::to_string(c.dropped) + " written=" +
           std::to_string(c.written) + " failed=" + std::to_string(c.failed) + " abandoned=" + std::to_string(c.abandoned) +
           " queued=" + std::to_string(c.queued) + " failing=" + (c.failing ? "true" : "false");
}

std::string counts_text(const Json& c) {
    auto n = [&](const char* key) { return std::to_string(static_cast<long long>(c.at(key).number)); };
    return std::string("accepted=") + n("accepted") + " dropped=" + n("dropped") + " written=" + n("written") +
           " failed=" + n("failed") + " abandoned=" + n("abandoned") + " queued=" + n("queued") +
           " failing=" + (c.at("failing").boolean ? "true" : "false");
}

void run_fixture(const Json& fixture, bool reporter) {
    const std::string mode = reporter ? "default reporter" : "callbacks";
    const Json& options = fixture.at("options");
    auto outage = std::make_shared<Outage>();
    outage->write_error = fixture.at("outage").at("write_error").text;
    outage->status = fixture.at("outage").at("rebind_error").at("status").text;
    outage->cause = fixture.at("outage").at("rebind_error").at("cause").text;
    logging::AsyncOptions async;
    async.capacity = static_cast<std::size_t>(options.at("capacity").number);
    async.initial_backoff = std::chrono::milliseconds(static_cast<long long>(options.at("initial_backoff_ms").number));
    async.max_backoff = std::chrono::milliseconds(static_cast<long long>(options.at("max_backoff_ms").number));
    async.program = options.at("program").text;
    std::mutex mutex;
    std::vector<std::string> transitions;
    std::exception_ptr failure;
    if (!reporter) {
        async.on_failure = [&](std::exception_ptr error, const logging::HandlerCounts&) {
            std::lock_guard<std::mutex> lock(mutex);
            failure = error;
            transitions.push_back("failure");
        };
        async.on_overflow = [&](const logging::HandlerCounts&) {
            std::lock_guard<std::mutex> lock(mutex);
            transitions.push_back("overflow");
        };
        async.on_recovery = [&](const logging::HandlerCounts&) {
            std::lock_guard<std::mutex> lock(mutex);
            transitions.push_back("recovery");
        };
    }
    auto captured = std::make_unique<CerrCapture>();
    logging::HandlerCounts final;
    std::vector<std::string> lines;
    {
        OA_LOGGING_OUTAGE_HANDLER handler(outage->writer(outage), outage->rebinder(outage), async);
        auto reported = [&]() -> std::size_t {
            if (reporter) return captured->lines().size();
            std::lock_guard<std::mutex> lock(mutex);
            return transitions.size();
        };
        int index = 0;
        for (const Json& step : fixture.at("steps").items) {
            const std::string where = mode + " step " + std::to_string(index++);
            if (const Json* defaults = step.find("defaults")) {
                const logging::AsyncOptions default_options;
                check(default_options.capacity == static_cast<std::size_t>(defaults->at("capacity").number), where + ": capacity default");
                check(default_options.initial_backoff.count() == defaults->at("initial_backoff_ms").number, where + ": initial backoff default");
                check(default_options.max_backoff.count() == defaults->at("max_backoff_ms").number, where + ": max backoff default");
                check(default_options.write_timeout.count() == defaults->at("write_timeout_ms").number, where + ": write timeout default");
                check(logging::async_detail::kBackoffJitter * 100 == defaults->at("jitter_percent").number, where + ": jitter default");
                check(logging::kGapMessage == defaults->at("gap_message").text, where + ": gap message default");
            } else if (const Json* log = step.find("log")) {
                for (const Json& message : log->items) handler.log(0, message.text);
            } else if (const Json* down = step.find("outage")) {
                outage->set(down->boolean);
            } else if (const Json* hold = step.find("hold")) {
                outage->hold_next(hold->text);
            } else if (step.find("release")) {
                outage->release_held();
            } else if (const Json* counts = step.find("counts")) {
                check(counts_text(handler.counts()) == counts_text(*counts), where + ": counts " + counts_text(handler.counts()));
            } else if (step.find("close")) {
                final = handler.close(5s);
            } else {
                const std::string await = step.at("await").text;
                bool ok = false;
                if (await == "settled") {
                    ok = eventually([&] {
                        auto c = handler.counts();
                        return c.queued == 0 && !c.failing && c.written + c.failed + c.abandoned == c.accepted;
                    });
                } else if (await == "failing") {
                    ok = eventually([&] { return handler.counts().failing && reported() >= 1; });
                } else if (await == "retry") {
                    ok = eventually([&] { return outage->rebind_count() >= 2; });
                } else if (await == "held") {
                    ok = eventually([&] { return outage->is_held(); });
                }
                check(ok, where + ": await " + await);
            }
        }
        lines = captured->lines();
    }
    captured.reset();
    const Json& expect = fixture.at("expect");
    check(counts_text(final) == counts_text(expect.at("counts")), mode + ": final counts " + counts_text(final));
    check(final.last_error.find(expect.at("last_error_contains").text) != std::string::npos, mode + ": last error " + final.last_error);
    auto delivered = outage->records();
    std::vector<std::string> messages;
    for (const auto& record : delivered) messages.push_back(record.msg);
    std::vector<std::string> wanted;
    for (const Json& message : expect.at("delivered").items) wanted.push_back(message.text);
    check(messages == wanted, mode + ": delivered order");
    if (delivered.size() > 2) {
        const auto& gap = delivered[2];
        const Json& want = expect.at("gap");
        check(gap.level == static_cast<std::int64_t>(want.at("level").number) && gap.msg == want.at("msg").text, mode + ": gap level and message");
        check(gap.identity.size() == 1 && gap.identity[0].hop == static_cast<std::int64_t>(want.at("hop").number) &&
                  gap.identity[0].by == want.at("by").text && gap.identity[0].program == want.at("program").text,
              mode + ": gap claim");
        for (const auto& member : want.at("attrs").members) {
            auto found = gap.attrs.find(member.first);
            check(found != gap.attrs.end() && found->second == member.second.text, mode + ": gap attribute " + member.first);
        }
        const std::regex instant(R"(^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$)");
        for (const Json& key : want.at("timestamps").items) {
            auto found = gap.attrs.find(key.text);
            check(found != gap.attrs.end() && std::regex_match(found->second, instant), mode + ": gap instant " + key.text);
        }
        check(gap.attrs.at("since") <= gap.attrs.at("until"), mode + ": gap since after until");
    }
    // Every drop is reported by exactly one gap record; a later gap record begins
    // where the previous one ended.
    {
        const Json& want = expect.at("gap");
        const std::regex instant(R"(^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$)");
        std::vector<std::string> gaps;
        std::uint64_t reported_total = 0;
        std::string previous_until;
        for (auto record : delivered) {
            if (record.msg != want.at("msg").text) continue;
            gaps.push_back(record.attrs["dropped"]);
            reported_total += std::stoull(record.attrs["dropped"]);
            for (const Json& key : want.at("timestamps").items)
                check(std::regex_match(record.attrs[key.text], instant), mode + ": gap instant " + key.text);
            check(record.attrs["since"] <= record.attrs["until"], mode + ": gap since after until");
            if (!previous_until.empty())
                check(record.attrs["since"] == previous_until, mode + ": later gap since " + record.attrs["since"] +
                                                                   ", want the previous until " + previous_until);
            previous_until = record.attrs["until"];
        }
        std::vector<std::string> wanted_gaps;
        for (const Json& dropped : expect.at("gaps_dropped").items) wanted_gaps.push_back(dropped.text);
        check(gaps == wanted_gaps, mode + ": gap records' dropped counts");
        check(reported_total == final.dropped, mode + ": gap records report " + std::to_string(reported_total) +
                                                   " dropped, counts say " + std::to_string(final.dropped));
    }
    if (reporter) {
        std::vector<std::string> wanted_lines;
        for (const Json& line : expect.at("reporter").items) wanted_lines.push_back(line.text);
        check(lines == wanted_lines, mode + ": reporter lines");
        if (lines != wanted_lines)
            for (const auto& line : lines) std::cout << "  got: " << line << "\n";
    } else {
        std::vector<std::string> wanted_transitions;
        for (const Json& name : expect.at("transitions").items) wanted_transitions.push_back(name.text);
        check(transitions == wanted_transitions, mode + ": transitions");
        check(lines.empty(), mode + ": callbacks wrote to std::cerr");
        check(logging::async_detail::failure_class(failure) == expect.at("failure_status").text, mode + ": failure status");
    }
}

// A 2,000-record burst while the service is down drops exactly the newest
// 2000 - 1024, reports the overflow once, and the gap record carries the count.
void burst_during_an_outage() {
    auto outage = std::make_shared<Outage>();
    int overflows = 0;
    logging::AsyncOptions async;
    async.initial_backoff = 5ms;
    async.max_backoff = 20ms;
    async.on_overflow = [&](const logging::HandlerCounts&) { ++overflows; };
    async.on_failure = [](std::exception_ptr, const logging::HandlerCounts&) {};
    logging::AsyncHandler handler(outage->writer(outage), outage->rebinder(outage), async);
    outage->set(true);
    for (int i = 0; i < 2000; ++i) handler.log(0, "b" + std::to_string(i));
    auto during = handler.counts();
    check(during.accepted == 1024 && during.dropped == 976 && during.queued == 1024, "burst: counts during the outage " + counts_text(during));
    check(eventually([&] { return handler.counts().failing && outage->rebind_count() >= 2; }), "burst: a retry during the outage");
    outage->set(false);
    auto final = handler.close(10s);
    check(final.written == 1024 && final.failed == 0 && final.dropped == 976, "burst: final counts " + counts_text(final));
    auto delivered = outage->records();
    check(delivered.size() == 1025 && delivered[0].msg == logging::kGapMessage && delivered[0].attrs["dropped"] == "976" &&
              delivered[1].msg == "b0" && delivered[1024].msg == "b1023",
          "burst: gap first with the dropped count, then the queue in order; delivered " + std::to_string(delivered.size()) +
              (delivered.empty() ? std::string() : ", first " + delivered[0].msg + " dropped=" + delivered[0].attrs["dropped"] +
                                                       ", second " + (delivered.size() > 1 ? delivered[1].msg : std::string())));
    check(overflows == 1, "burst: overflow reported " + std::to_string(overflows) + " times");
}

// Close at its timeout during an outage counts the head Failed and the rest Abandoned.
void close_at_the_deadline_during_an_outage() {
    auto outage = std::make_shared<Outage>();
    outage->set(true);
    logging::AsyncOptions async;
    async.initial_backoff = 5ms;
    async.max_backoff = 10ms;
    async.on_failure = [](std::exception_ptr, const logging::HandlerCounts&) {};
    logging::AsyncHandler handler(outage->writer(outage), outage->rebinder(outage), async);
    for (int i = 0; i < 5; ++i) handler.log(0, "a" + std::to_string(i));
    check(eventually([&] { return handler.counts().failing; }), "close: the failure");
    const auto began = std::chrono::steady_clock::now();
    auto c = handler.close(100ms);
    check(std::chrono::steady_clock::now() - began < 2s, "close: returned near its deadline");
    check(c.accepted == 5 && c.failed == 1 && c.abandoned == 4 && c.written == 0 && c.queued == 0, "close: counts " + counts_text(c));
}

// A hung writer costs write() nothing, times out as a failure of class timeout,
// and its records are delivered once it returns.
void hung_writer() {
    struct Gate {
        std::mutex mutex;
        std::condition_variable open_signal;
        bool open = false;
        std::vector<std::string> delivered;
    };
    auto gate = std::make_shared<Gate>();
    std::mutex mutex;
    std::exception_ptr failure;
    logging::AsyncOptions async;
    async.write_timeout = 30ms;
    async.initial_backoff = 1ms;
    async.max_backoff = 2ms;
    async.on_failure = [&](std::exception_ptr error, const logging::HandlerCounts&) {
        std::lock_guard<std::mutex> lock(mutex);
        failure = error;
    };
    logging::AsyncHandler handler(
        [gate](const logging::Record& record) {
            std::unique_lock<std::mutex> lock(gate->mutex);
            gate->open_signal.wait(lock, [&] { return gate->open; });
            gate->delivered.push_back(record.msg);
        },
        {}, async);
    std::vector<std::chrono::steady_clock::duration> latencies;
    for (int i = 0; i < 100; ++i) {
        const auto began = std::chrono::steady_clock::now();
        handler.log(0, "h" + std::to_string(i));
        latencies.push_back(std::chrono::steady_clock::now() - began);
    }
    std::sort(latencies.begin(), latencies.end());
    // The 95th percentile, so one scheduler preemption on a loaded host does not decide it.
    check(latencies[95] < 1ms, "hung writer: write() 95th percentile " +
                                   std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(latencies[95]).count()) + " us");
    check(eventually([&] {
        std::lock_guard<std::mutex> lock(mutex);
        return failure != nullptr;
    }), "hung writer: the timeout failure");
    {
        std::lock_guard<std::mutex> lock(mutex);
        check(logging::async_detail::failure_class(failure) == "timeout", "hung writer: failure class");
    }
    check(handler.counts().failed == 0 && handler.counts().queued == 100, "hung writer: records stay queued " + counts_text(handler.counts()));
    {
        std::lock_guard<std::mutex> lock(gate->mutex);
        gate->open = true;
    }
    gate->open_signal.notify_all();
    auto c = handler.close(10s);
    check(c.written == 100 && c.failed == 0 && !c.failing, "hung writer: final counts " + counts_text(c));
}

void nonpositive_options() {
    const logging::AsyncOptions defaults;
    for (const auto invalid : {0ms, -1ms}) {
        logging::AsyncOptions original;
        original.capacity = 0;
        original.initial_backoff = invalid;
        original.max_backoff = invalid;
        original.write_timeout = invalid;
        auto normalized = original;
        logging::async_detail::normalize_options(normalized);
        check(normalized.capacity == defaults.capacity, "nonpositive options: capacity");
        check(normalized.initial_backoff == defaults.initial_backoff, "nonpositive options: initial backoff");
        check(normalized.max_backoff == defaults.max_backoff, "nonpositive options: maximum backoff");
        check(normalized.write_timeout == invalid, "nonpositive options: unbounded delivery preserved");
        logging::AsyncHandler handler([](const logging::Record&) {}, {}, original);
        handler.log(0, "normalized options");
        const auto counts = handler.close(1s);
        check(counts.written == 1 && counts.dropped == 0, "nonpositive options: constructor uses normalization");
    }
    logging::AsyncOptions custom;
    custom.capacity = 3;
    custom.initial_backoff = 7ms;
    custom.max_backoff = 11ms;
    custom.write_timeout = 13ms;
    logging::async_detail::normalize_options(custom);
    check(custom.capacity == 3 && custom.initial_backoff == 7ms && custom.max_backoff == 11ms && custom.write_timeout == 13ms,
          "positive options: explicit values preserved");
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: sink_outage <conformance/faults/sink-outage/sink-outage.json>\n";
        return 2;
    }
    std::ifstream in(argv[1], std::ios::binary);
    if (!in) {
        std::cerr << "cannot open " << argv[1] << "\n";
        return 2;
    }
    const Json fixture = JsonReader(std::string(std::istreambuf_iterator<char>(in), {})).read();
    run_fixture(fixture, false);
    run_fixture(fixture, true);
    burst_during_an_outage();
    close_at_the_deadline_during_an_outage();
    hung_writer();
    nonpositive_options();
    if (failures) {
        std::cerr << failures << " failures\n";
        return 1;
    }
    std::cout << "PASS sink-outage fixture (callbacks, default reporter), burst, close deadline, hung writer\n";
    return 0;
}
