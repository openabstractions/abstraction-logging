// testdata/records/unclaimed-hop0.jsonl through the generated C++ reader [LOG-I16].
// C++ reads and writes records; no C++ component accepts records as a receiving
// service. This proves the reader keeps the service's unclaimed hop 0.
// Run: unclaimed_hop0 <abstraction-logging>/testdata/records/unclaimed-hop0.jsonl
#include "abstraction/logging/rec.h"

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

namespace {
bool check(const abstraction::logging::Record& record, const char* stage) {
    if (record.identity.size() != 2) {
        std::cerr << stage << ": " << record.identity.size() << " hops\n";
        return false;
    }
    const auto& first = record.identity[0];
    const auto& stamp = record.identity[1];
    if (first.by != "unclaimed" || first.verified || first.hop != 0 || first.uid != -1 || first.gid != -1 ||
        first.pid != -1 || !first.program.empty() || !first.exe.empty()) {
        std::cerr << stage << ": hop 0 by=" << first.by << " hop=" << first.hop << "\n";
        return false;
    }
    if (stamp.by != "identity/windows" || !stamp.verified || stamp.hop != 1 ||
        stamp.exe != "C:\\Users\\oa\\Python312\\python.exe") {
        std::cerr << stage << ": hop 1 by=" << stamp.by << " hop=" << stamp.hop << "\n";
        return false;
    }
    auto claim = record.attrs.find("logging.writer_claim");
    if (record.attrs.size() != 1 || claim == record.attrs.end() || claim->second != "absent") {
        std::cerr << stage << ": attrs differ\n";
        return false;
    }
    return true;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: unclaimed_hop0 <unclaimed-hop0.jsonl>\n";
        return 2;
    }
    std::ifstream input(argv[1], std::ios::binary);
    if (!input) {
        std::cerr << "cannot read " << argv[1] << "\n";
        return 2;
    }
    std::string text((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    try {
        auto record = abstraction::logging::decode(text);
        if (!check(record, "decode")) return 1;
        if (!check(abstraction::logging::decode(abstraction::logging::encode(record)), "round trip")) return 1;
    } catch (const std::exception& error) {
        std::cerr << "refused: " << error.what() << "\n";
        return 1;
    }
    std::cout << "PASS C++ reader: unclaimed hop 0 and writer_claim=absent kept\n";
    return 0;
}
