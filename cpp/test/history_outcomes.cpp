#include "abstraction/logging/client.hpp"
#include "abstraction/logging/rec.h"
#include <type_traits>
#include <utility>
#include <iostream>
#include <stdexcept>

static void require(bool condition) {
    if (!condition) throw std::runtime_error("history outcome codec regression");
}

int main() {
    using namespace abstraction::logging;
    for (const auto& name : kHistoryPageOutcomeNames) {
        SourcePage page;
        page.outcome = *parse_history_page_outcome(name);
        page.next = "supplied";
        const auto result = decode_source_page(encode_source_page(page));
        require(result.outcome == page.outcome && result.next == page.next && result.records.empty() && !result.at_end);
    }
    const auto catalog = decode_source_catalog(R"({"sources":[{"id":"windows/application","availability":"available","limitations":["future_constraint"]}],"outcome":"catalog"})");
    require(catalog.outcome == SourceCatalogOutcome::Catalog && catalog.sources[0].limitations[0] == "future_constraint");
    require(encode_source_catalog(catalog).find("future_constraint") != std::string::npos);
    for (const auto* raw : {R"({"sources":[]})", R"({"sources":[],"outcome":"future_action"})", R"({"sources":[{"id":"windows/application","availability":"future_state","limitations":[]}],"outcome":"catalog"})"}) {
        bool refused = false;
        try { decode_source_catalog(raw); } catch (const Refusal&) { refused = true; }
        require(refused);
    }
    bool refused = false;
    try { decode_source_page(R"({"outcome":"future_action","records":[],"next":"same","at_end":false})"); } catch (const Refusal&) { refused = true; }
    require(refused);
    Page legacy;
    legacy.outcome = PageOutcome::InvalidRequest;
    legacy.next = "supplied";
    require(encode_page(legacy) == "{\n  \"outcome\": \"invalid_request\",\n  \"records\": [],\n  \"next\": \"supplied\",\n  \"at_end\": false\n}\n");
    static_assert(std::is_same_v<decltype(std::declval<const History&>().usage()), HistoryUsageResult>);
    HistoryUsageResult usage;
    usage.outcome = HistoryUsageOutcome::Available;
    usage.usage = HistoryUsage{42, "2026-09-13T00:00:00Z", 1024, std::nullopt};
    const auto available = from_json<HistoryUsageResult>(to_json(usage));
    require(available.outcome == HistoryUsageOutcome::Available && available.usage && available.usage->retained_bytes == 42 && !available.usage->max_retention_ms);
    usage = HistoryUsageResult{};
    usage.outcome = HistoryUsageOutcome::Unavailable;
    require(!from_json<HistoryUsageResult>(to_json(usage)).usage);
    std::cout << "history outcome codec checks PASS\n";
}
