use abstraction_logging_api::*;

#[test]
fn typed_history_outcomes_and_descriptive_constraints() {
    for outcome in HistoryPageOutcome::ALL {
        let page = SourcePage { outcome, records: Vec::new(), next: "supplied".into(), at_end: false };
        let encoded = encode_source_page_document(&page).unwrap();
        assert_eq!(decode_source_page_document(&encoded).unwrap(), page);
    }
    let raw = br#"{"sources":[{"id":"windows/application","availability":"available","limitations":["future_constraint"]}],"outcome":"catalog"}"#;
    let catalog = decode_source_catalog_document(raw).unwrap();
    assert_eq!(catalog.outcome, SourceCatalogOutcome::Catalog);
    assert_eq!(catalog.sources[0].limitations, vec!["future_constraint".to_string()]);
    assert_eq!(decode_source_catalog_document(&encode_source_catalog_document(&catalog).unwrap()).unwrap(), catalog);
    assert!(decode_source_page_document(br#"{"outcome":"future_action","records":[],"next":"same","at_end":false}"#).is_err());
    assert!(decode_source_catalog_document(br#"{"sources":[]}"#).is_err());
    assert!(decode_source_catalog_document(br#"{"sources":[],"outcome":"future_action"}"#).is_err());
    assert!(decode_source_descriptor_document(br#"{"id":"windows/application","availability":"future_state","limitations":[]}"#).is_err());
    let legacy = Page { outcome: PageOutcome::InvalidRequest, records: Vec::new(), next: "supplied".into(), at_end: false };
    assert_eq!(encode_page_document(&legacy).unwrap(), b"{\n  \"outcome\": \"invalid_request\",\n  \"records\": [],\n  \"next\": \"supplied\",\n  \"at_end\": false\n}\n");
}
