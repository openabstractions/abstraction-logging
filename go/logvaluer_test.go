package logging

import (
	"log/slog"
	"testing"
)

// faultLike is a value that logs itself as a group, the way gpufault.Fault
// does.
type faultLike struct{ line, engines string }

func (f faultLike) LogValue() slog.Value {
	return slog.GroupValue(slog.String("line", f.line), slog.String("engines", f.engines))
}

// scalarLike logs itself as one string.
type scalarLike struct{ word string }

func (s scalarLike) LogValue() slog.Value { return slog.StringValue("word:" + s.word) }

// A slog.LogValuer is resolved before flattening: a group it returns becomes
// dotted keys, a scalar its own value. Unresolved, the record carried the Go
// struct's fmt text under the attribute's key and no dotted key at all, so a
// reader looking for gpu_fault.line found nothing (RES-G6).
func TestLogValuerResolvesBeforeFlattening(t *testing.T) {
	for name, log := range map[string]func(*slog.Logger){
		"record": func(l *slog.Logger) {
			l.Warn("gpu fault", slog.Any("gpu_fault", faultLike{line: "The graphics driver reset.", engines: "sd-server"}), slog.Any("word", scalarLike{"x"}))
		},
		"with": func(l *slog.Logger) {
			l.With(slog.Any("gpu_fault", faultLike{line: "The graphics driver reset.", engines: "sd-server"}), slog.Any("word", scalarLike{"x"})).Warn("gpu fault")
		},
		"group": func(l *slog.Logger) {
			l.WithGroup("outer").Warn("gpu fault", slog.Any("gpu_fault", faultLike{line: "The graphics driver reset.", engines: "sd-server"}), slog.Any("word", scalarLike{"x"}))
		},
	} {
		sink := &recordingSink{}
		log(slog.New(NewHandler(sink, &Options{Program: "p"})))
		if len(sink.records) != 1 {
			t.Fatalf("%s: %d records", name, len(sink.records))
		}
		prefix := ""
		if name == "group" {
			prefix = "outer."
		}
		attrs := sink.records[0].Attrs
		if attrs[prefix+"gpu_fault.line"] != "The graphics driver reset." || attrs[prefix+"gpu_fault.engines"] != "sd-server" ||
			attrs[prefix+"word"] != "word:x" {
			t.Fatalf("%s: attrs %v", name, attrs)
		}
		if _, ok := attrs[prefix+"gpu_fault"]; ok {
			t.Fatalf("%s: the unresolved value was kept beside its keys: %v", name, attrs)
		}
	}
}
