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
private:
    std::string endpoint_;
    std::optional<ipc::Deadline> deadline_;
};
}
