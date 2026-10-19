#include "abstraction/logging/rec.h"
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
    std::cout << "history outcome codec checks PASS\n";
}
