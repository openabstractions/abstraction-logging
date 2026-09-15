// testdata/records/attestation-fields.jsonl through the generated C++ reader.
// Build: c++ -std=c++17 -I<abstraction-logging>/cpp attestation_fields.cpp
// Run:   attestation_fields <abstraction-logging>/testdata/records/attestation-fields.jsonl
#include "abstraction/logging/rec.h"

#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

namespace {
struct Hop {
    std::string by;
    bool verified;
    std::int64_t hop;
    std::string program, user, exe;
    std::int64_t uid;
};

bool same(const abstraction::logging::Attestation& a, const Hop& want) {
    return a.by == want.by && a.verified == want.verified && a.hop == want.hop && a.program == want.program &&
           a.user == want.user && a.exe == want.exe && a.uid == want.uid;
}

bool check(const abstraction::logging::Record& record, const char* stage) {
    const std::vector<Hop> want = {
        {"self", false, 0, "ComfyUI", "", "", -1},
        {"identity/windows", true, 1, "", "S-1-5-21-1-2-3-1001", "C:\\Users\\oa\\Python312\\python.exe", -1},
        {"so_peercred", true, 2, "", "oa", "/usr/bin/python3.14", 1000},
    };
    if (record.identity.size() != want.size()) {
        std::cerr << stage << ": " << record.identity.size() << " hops\n";
        return false;
    }
    for (std::size_t i = 0; i < want.size(); ++i) {
        if (!same(record.identity[i], want[i])) {
            std::cerr << stage << ": hop " << i << " differs (by=" << record.identity[i].by
                      << " program=" << record.identity[i].program << " exe=" << record.identity[i].exe << ")\n";
            return false;
        }
    }
    return true;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: attestation_fields <attestation-fields.jsonl>\n";
        return 2;
    }
    std::ifstream input(argv[1], std::ios::binary);
    std::string text((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    if (!input && !input.eof()) {
        std::cerr << "cannot read " << argv[1] << "\n";
        return 2;
    }
    try {
        auto record = abstraction::logging::decode(text);
        if (!check(record, "decode")) return 1;
        auto again = abstraction::logging::decode(abstraction::logging::encode(record));
        if (!check(again, "round trip")) return 1;
    } catch (const std::exception& error) {
        std::cerr << "refused: " << error.what() << "\n";
        return 1;
    }
    std::cout << "PASS C++ reader: program on the writer claim, exe on each attester stamp\n";
    return 0;
}
