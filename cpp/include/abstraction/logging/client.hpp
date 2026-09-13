#pragma once
#include <abstraction/logging/rec.h>
#include <abstraction/ipc/frame.hpp>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <ctime>

namespace abstraction::logging {

inline std::string default_endpoint() {
    if (const char* value = std::getenv("ABSTRACTION_LOG_ENDPOINT")) {
        if (*value) return value;
    }
#ifdef _WIN32
    return R"(\\.\pipe\openabstractions-logging-v1)";
#else
    if (const char* value = std::getenv("XDG_RUNTIME_DIR")) {
        if (*value) return std::string(value) + "/openabstractions-logging-v1.sock";
    }
    const char* temporary = std::getenv("TMPDIR");
    const char* user = std::getenv("USER");
    return std::string(temporary && *temporary ? temporary : "/tmp") +
        "/openabstractions-logging-v1-" + (user ? user : "") + ".sock";
#endif
}

// A capability client: no sink, store, listener, or fallback provider.
class Logger {
public:
    explicit Logger(std::string endpoint = default_endpoint()) : endpoint_(std::move(endpoint)) {}

    // Explicit operation scope; copies retain the same absolute deadline.
    Logger(std::string endpoint, ipc::Deadline deadline)
        : endpoint_(std::move(endpoint)), deadline_(deadline) {}

    void Write(const Record& record) const {
        auto transport = deadline_ ? ipc::FrameTransport(endpoint_, *deadline_, 1 << 20)
                                   : ipc::FrameTransport(endpoint_, 2000, 1 << 20);
        transport = transport.WithCancellation(cancellation_).WithServerExpectation(server_);
        SinkClient<ipc::FrameTransport> client(transport);
        client.Write(record);
    }

    // Applications supply the event; schema and timestamp are protocol details.
    void Log(std::int64_t level, std::string message,
             std::map<std::string, std::string> attributes = {}) const {
        const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        const std::time_t seconds = static_cast<std::time_t>(micros / 1000000);
        std::tm utc{};
#ifdef _WIN32
        if (gmtime_s(&utc, &seconds)) throw std::runtime_error("logging: invalid clock");
#else
        if (!gmtime_r(&seconds, &utc)) throw std::runtime_error("logging: invalid clock");
#endif
        char date[32], timestamp[48];
        if (!std::strftime(date, sizeof(date), "%Y-%m-%dT%H:%M:%S", &utc))
            throw std::runtime_error("logging: invalid clock");
        std::snprintf(timestamp, sizeof(timestamp), "%s.%06lldZ", date,
                      static_cast<long long>(micros % 1000000));
        Record record;
        record.schema = 1;
        record.time = timestamp;
        record.level = level;
        record.msg = std::move(message);
        record.attrs = std::move(attributes);
        Attestation self;
        self.by = "self";
        self.uid = self.gid = self.pid = -1;
        record.identity.push_back(std::move(self));
        Write(record);
    }
    Logger WithServerExpectation(std::optional<ipc::ServerExpectation> server) const {auto copy=*this;copy.server_=std::move(server);return copy;}
 Logger WithCancellation(ipc::CancellationToken token) const {
        auto scoped = *this;
        scoped.cancellation_ = std::move(token);
        return scoped;
    }
private:
    std::string endpoint_;
    std::optional<ipc::Deadline> deadline_;
    ipc::CancellationToken cancellation_;
 std::optional<ipc::ServerExpectation> server_;
};
namespace history_detail {
inline void validate(const Page& page,const std::string& cursor,std::int64_t max_records,std::int64_t max_bytes) {
  if(page.outcome=="page") {
   std::size_t bytes=0;
   for(const auto& record:page.records) bytes+=encode(record).size();
   if(max_records<1 || max_records>256 || max_bytes<1 || max_bytes>65536 || page.records.size()>static_cast<std::size_t>(max_records) || bytes>static_cast<std::size_t>(max_bytes) || page.next.empty() || (!page.records.empty() && page.next==cursor) || (!page.at_end && page.records.empty()))
    throw std::runtime_error("logging: invalid bounded history response");
  } else if(!page.records.empty() || page.next!=cursor || page.at_end) {
   throw std::runtime_error("logging: refusal changed history continuation");
  }
}
}
// A history binding retains its provider and optional caller waiting budget.
class History {
public:
 explicit History(std::string endpoint):endpoint_(std::move(endpoint)) {}
 History(std::string endpoint,ipc::Deadline deadline):endpoint_(std::move(endpoint)),deadline_(deadline) {}
 Page Read(const std::string& cursor, std::int64_t max_records=64, std::int64_t max_bytes=65536) const {
  auto transport=deadline_ ? ipc::FrameTransport(endpoint_,*deadline_,1<<20) : ipc::FrameTransport(endpoint_,2000,1<<20);
  transport=transport.WithCancellation(cancellation_).WithServerExpectation(server_);
  HistoryReaderClient<ipc::FrameTransport> client(transport);
  auto page=client.Read(cursor,max_records,max_bytes);
  history_detail::validate(page,cursor,max_records,max_bytes);
  return page;
 }
 History WithServerExpectation(std::optional<ipc::ServerExpectation> server) const {auto copy=*this;copy.server_=std::move(server);return copy;}
 History WithCancellation(ipc::CancellationToken token) const {
  auto scoped=*this;scoped.cancellation_=std::move(token);return scoped;
 }
private:
 std::string endpoint_;
 std::optional<ipc::Deadline> deadline_;
 ipc::CancellationToken cancellation_;
 std::optional<ipc::ServerExpectation> server_;
};

// Bounded long-poll observation of the selected history instance.
class Observer {
public:
 explicit Observer(std::string endpoint):endpoint_(std::move(endpoint)) {}
 Observer(std::string endpoint,ipc::Deadline deadline):endpoint_(std::move(endpoint)),deadline_(deadline) {}
 Page Observe(const std::string& cursor,std::int64_t max_records=64,std::int64_t max_bytes=65536,std::int64_t wait_ms=0)const {
  if(wait_ms<0||wait_ms>30000)throw std::invalid_argument("logging: wait_ms must be 0..30000");
  auto transport=deadline_ ? ipc::FrameTransport(endpoint_,*deadline_,1<<20) : ipc::FrameTransport(endpoint_,2000,1<<20);
  transport=transport.WithCancellation(cancellation_).WithServerExpectation(server_);
  HistoryObserverClient<ipc::FrameTransport> client(transport);
  auto page=client.Observe(cursor,max_records,max_bytes,wait_ms);
  history_detail::validate(page,cursor,max_records,max_bytes);return page;
 }
 Observer WithServerExpectation(std::optional<ipc::ServerExpectation> server) const {auto copy=*this;copy.server_=std::move(server);return copy;}
 Observer WithCancellation(ipc::CancellationToken token)const {auto copy=*this;copy.cancellation_=std::move(token);return copy;}
private:
 std::string endpoint_;
 std::optional<ipc::Deadline> deadline_;
 ipc::CancellationToken cancellation_;
 std::optional<ipc::ServerExpectation> server_;
};
}
