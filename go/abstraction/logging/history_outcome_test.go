package logging

import (
	"bytes"
	"testing"
)

func TestHistoryPageOutcomesRoundTripAndRefuseUnknown(t *testing.T) {
	for _, outcome := range HistoryPageOutcomeValues() {
		page := SourcePage{Outcome: outcome, Records: []SourceRecord{}, Next: "supplied"}
		decoded, err := DecodeSourcePage(EncodeSourcePage(&page))
		if err != nil || decoded.Outcome != outcome || decoded.Next != "supplied" || len(decoded.Records) != 0 || decoded.AtEnd {
			t.Fatalf("%s: %+v, %v", outcome, decoded, err)
		}
		application := ApplicationLogPage{Outcome: outcome, Records: []Record{}, Next: "supplied"}
		result, err := DecodeApplicationLogPage(EncodeApplicationLogPage(&application))
		if err != nil || result.Outcome != outcome || result.Next != "supplied" {
			t.Fatalf("application %s: %+v, %v", outcome, result, err)
		}
	}
	for _, decode := range []func([]byte) error{
		func(data []byte) error { _, err := DecodeSourcePage(data); return err },
		func(data []byte) error { _, err := DecodeApplicationLogPage(data); return err },
	} {
		if err := decode([]byte(`{"outcome":"future_action","records":[],"next":"same","at_end":false}`)); err == nil {
			t.Fatal("unknown outcome selected behavior")
		}
	}
}

func TestSourceCatalogOutcomeAndDescriptiveVocabulary(t *testing.T) {
	for _, outcome := range SourceCatalogOutcomeValues() {
		catalog := SourceCatalog{Outcome: outcome, Sources: []SourceDescriptor{}}
		decoded, err := DecodeSourceCatalog(EncodeSourceCatalog(&catalog))
		if err != nil || decoded.Outcome != outcome {
			t.Fatalf("%s: %+v, %v", outcome, decoded, err)
		}
	}
	for _, invalid := range []string{
		`{"sources":[]}`,
		`{"sources":[],"outcome":"future_action"}`,
		`{"sources":[{"id":"windows/application","availability":"future_state","limitations":[]}],"outcome":"catalog"}`,
	} {
		if _, err := DecodeSourceCatalog([]byte(invalid)); err == nil {
			t.Fatalf("accepted %s", invalid)
		}
	}
	data := []byte(`{"sources":[{"id":"windows/application","availability":"available","limitations":["future_constraint"]}],"outcome":"catalog"}`)
	catalog, err := DecodeSourceCatalog(data)
	if err != nil || !bytes.Contains(EncodeSourceCatalog(catalog), []byte(`"future_constraint"`)) {
		t.Fatalf("descriptive constraint: %+v, %v", catalog, err)
	}
}

func TestPublishedPageWireShapeRemains(t *testing.T) {
	page := Page{Outcome: PageOutcomeInvalidRequest, Records: []Record{}, Next: "supplied"}
	want := []byte("{\n  \"outcome\": \"invalid_request\",\n  \"records\": [],\n  \"next\": \"supplied\",\n  \"at_end\": false\n}\n")
	if got := EncodePage(&page); !bytes.Equal(got, want) {
		t.Fatalf("published page shape changed:\n%s", got)
	}
	if _, err := DecodePage([]byte(`{"outcome":"invalid","records":[],"next":"supplied","at_end":false}`)); err == nil {
		t.Fatal("new vocabulary entered legacy Page")
	}
}
