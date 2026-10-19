//go:build windows

package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

type windowsEventSource struct {
	id       string
	channel  string
	runner   nativeCommandRunner
	pathOnce sync.Once
	path     string
	pathErr  error
}

func nativePlatformSourceAdapters(runner nativeCommandRunner) map[string]SourceAdapter {
	result := make(map[string]SourceAdapter)
	for id, channel := range map[string]string{
		"windows/application": "Application",
		"windows/system":      "System",
		"windows/security":    "Security",
	} {
		result[id] = &windowsEventSource{id: id, channel: channel, runner: runner}
	}
	return result
}

func (s *windowsEventSource) ID() string { return s.id }

func (s *windowsEventSource) Describe(context.Context) (wire.SourceDescriptor, error) {
	if _, err := s.resolvePath(); err != nil {
		return wire.SourceDescriptor{}, err
	}
	return wire.SourceDescriptor{ID: s.id, Availability: wire.SourceAvailabilityAvailable, Limitations: []wire.SourceLimitation{
		wire.SourceLimitationRetention, wire.SourceLimitationCollectionPolicy, wire.SourceLimitationNativeAccess,
	}}, nil
}

func (s *windowsEventSource) Read(ctx context.Context, cursor string, maxRecords, maxBytes int64) (NativePage, error) {
	path, err := s.resolvePath()
	if err != nil {
		return NativePage{}, err
	}
	var after uint64
	if cursor != "" {
		var wantHash string
		var err error
		after, wantHash, err = decodeWindowsEventCursor(cursor)
		if err != nil {
			return NativePage{Outcome: wire.PageOutcomeGap}, nil
		}
		anchor, err := s.query(ctx, path, fmt.Sprintf("*[System[EventRecordID=%d]]", after), 1, false)
		if err != nil {
			if noWindowsEvents(err) {
				return NativePage{Outcome: wire.PageOutcomeGap}, nil
			}
			return NativePage{}, windowsEventError(err)
		}
		if len(anchor) != 1 || anchor[0].cursorHash() != wantHash {
			return NativePage{Outcome: wire.PageOutcomeGap}, nil
		}
	}
	var query string
	if cursor != "" {
		query = fmt.Sprintf("*[System[EventRecordID>%d]]", after)
	}
	items, err := s.query(ctx, path, query, int(maxRecords+1), false)
	if err != nil {
		if noWindowsEvents(err) {
			return NativePage{Outcome: wire.PageOutcomePage, Next: cursor, AtEnd: true}, nil
		}
		return NativePage{}, windowsEventError(err)
	}
	entries := make([]nativeSourceEntry, 0, len(items))
	for _, item := range items {
		entry, err := item.sourceEntry()
		if err != nil {
			return NativePage{Outcome: wire.PageOutcomeCorrupt}, nil
		}
		entries = append(entries, nativeSourceEntry{entry: entry, cursor: item.cursor()})
	}
	return boundedNativePage(s.id, entries, cursor, maxRecords, maxBytes), nil
}

func (s *windowsEventSource) CurrentCursor(ctx context.Context) (string, error) {
	path, err := s.resolvePath()
	if err != nil {
		return "", err
	}
	items, err := s.query(ctx, path, "", 1, true)
	if err != nil {
		if noWindowsEvents(err) {
			return "", nil
		}
		return "", windowsEventError(err)
	}
	if len(items) == 0 {
		return "", nil
	}
	if len(items) != 1 || items[0].System.EventRecordID == 0 {
		return "", errors.New("Windows current position returned invalid event")
	}
	return items[0].cursor(), nil
}

func (s *windowsEventSource) resolvePath() (string, error) {
	s.pathOnce.Do(func() {
		if s.path != "" {
			return
		}
		path, err := s.runner.CommandPath("wevtutil.exe")
		if err != nil {
			s.pathErr = fmt.Errorf("Windows Event Log query tool unavailable: %w", err)
			return
		}
		s.path = path
	})
	return s.path, s.pathErr
}

func (s *windowsEventSource) query(ctx context.Context, path, xpath string, count int, reverse bool) ([]windowsEvent, error) {
	args := []string{"qe", s.channel}
	if xpath != "" {
		args = append(args, "/q:"+xpath)
	}
	args = append(args, fmt.Sprintf("/c:%d", count), fmt.Sprintf("/rd:%t", reverse), "/f:xml", "/e:Events", "/uni:true")
	output, err := s.runner.Run(ctx, path, args...)
	if err != nil {
		return nil, err
	}
	decoded, err := decodeWindowsCommandOutput(output)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(decoded)) == "" {
		return []windowsEvent{}, nil
	}
	var result windowsEvents
	if err := xml.Unmarshal(decoded, &result); err != nil {
		return nil, err
	}
	return result.Events, nil
}

type windowsEvents struct {
	XMLName xml.Name       `xml:"Events"`
	Events  []windowsEvent `xml:"Event"`
}

type windowsEvent struct {
	XMLName xml.Name `xml:"Event"`
	RawXML  string   `xml:",innerxml"`
	System  struct {
		Provider struct {
			Name string `xml:"Name,attr"`
		} `xml:"Provider"`
		EventID     uint64 `xml:"EventID"`
		Level       int64  `xml:"Level"`
		Task        uint64 `xml:"Task"`
		TimeCreated struct {
			SystemTime string `xml:"SystemTime,attr"`
		} `xml:"TimeCreated"`
		EventRecordID uint64 `xml:"EventRecordID"`
		Channel       string `xml:"Channel"`
		Execution     struct {
			ProcessID int64 `xml:"ProcessID,attr"`
		} `xml:"Execution"`
		Security struct {
			UserID string `xml:"UserID,attr"`
		} `xml:"Security"`
	} `xml:"System"`
	EventData struct {
		Data []struct {
			Name  string `xml:"Name,attr"`
			Value string `xml:",chardata"`
		} `xml:"Data"`
	} `xml:"EventData"`
}

func (e windowsEvent) cursorHash() string {
	value := []byte(e.RawXML)
	if len(value) == 0 {
		value, _ = xml.Marshal(e)
	}
	hash := sha256.Sum256(value)
	return hex.EncodeToString(hash[:])
}

func (e windowsEvent) cursor() string {
	return strconv.FormatUint(e.System.EventRecordID, 10) + ":" + e.cursorHash()
}

func (e windowsEvent) sourceEntry() (SourceEntry, error) {
	timestamp, err := time.Parse(time.RFC3339Nano, e.System.TimeCreated.SystemTime)
	if err != nil || e.System.EventRecordID == 0 {
		return SourceEntry{}, errors.New("invalid Windows event timestamp or record ID")
	}
	attrs := map[string]string{
		"CHANNEL":         e.System.Channel,
		"EVENT_ID":        strconv.FormatUint(e.System.EventID, 10),
		"EVENT_RECORD_ID": strconv.FormatUint(e.System.EventRecordID, 10),
		"TASK":            strconv.FormatUint(e.System.Task, 10),
	}
	parts := make([]string, 0, len(e.EventData.Data))
	for i, value := range e.EventData.Data {
		name := value.Name
		if name == "" {
			name = strconv.Itoa(i)
		}
		attrs["EVENT_DATA."+name] = value.Value
		parts = append(parts, value.Value)
	}
	message := fmt.Sprintf("Windows event %d", e.System.EventID)
	if len(parts) > 0 {
		message += ": " + strings.Join(parts, " ")
	}
	return SourceEntry{
		Record:   wire.Record{Schema: 1, Time: timestamp.UTC().Format(time.RFC3339Nano), Level: e.System.Level, Msg: message, Attrs: attrs},
		Producer: &wire.ProducerClaim{Provider: e.System.Provider.Name, PID: e.System.Execution.ProcessID, User: e.System.Security.UserID},
	}, nil
}

func decodeWindowsEventCursor(value string) (uint64, string, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 || len(parts[1]) != 64 {
		return 0, "", errors.New("invalid Windows event cursor")
	}
	id, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || id == 0 {
		return 0, "", errors.New("invalid Windows event record ID")
	}
	if _, err := hex.DecodeString(parts[1]); err != nil {
		return 0, "", errors.New("invalid Windows event anchor hash")
	}
	return id, parts[1], nil
}

func decodeWindowsCommandOutput(value []byte) ([]byte, error) {
	if len(value) >= 2 && (value[0] == 0xff && value[1] == 0xfe || value[0] == '<' && value[1] == 0) {
		if value[0] == 0xff {
			value = value[2:]
		} else {
			// wevtutil can omit the UTF-16 BOM when writing to a pipe.
		}
		if len(value)%2 != 0 {
			return nil, errors.New("truncated UTF-16 Windows Event Log output")
		}
		units := make([]uint16, len(value)/2)
		for i := range units {
			units[i] = uint16(value[i*2]) | uint16(value[i*2+1])<<8
		}
		return []byte(string(utf16.Decode(units))), nil
	}
	if len(value) >= 2 && (value[0] == 0xfe && value[1] == 0xff || value[0] == 0 && value[1] == '<') {
		if value[0] == 0xfe {
			value = value[2:]
		}
		if len(value)%2 != 0 {
			return nil, errors.New("truncated UTF-16 Windows Event Log output")
		}
		units := make([]uint16, len(value)/2)
		for i := range units {
			units[i] = uint16(value[1+i*2]) | uint16(value[i*2])<<8
		}
		return []byte(string(utf16.Decode(units))), nil
	}
	return bytes.TrimPrefix(value, []byte{0xef, 0xbb, 0xbf}), nil
}

func noWindowsEvents(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "no events found")
}

func windowsEventError(err error) error {
	text := strings.ToLower(err.Error())
	if strings.Contains(text, "access is denied") || strings.Contains(text, "access denied") || strings.Contains(text, "e_accessdenied") {
		return fmt.Errorf("%w: %v", ErrSourcePermissionDenied, err)
	}
	return err
}
