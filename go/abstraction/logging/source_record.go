package logging

// SourceRecordEncodedSize returns the generated wire size used by source page
// byte limits. It includes the source ID and producer claims.
func SourceRecordEncodedSize(record *SourceRecord) int {
	if record == nil {
		return 0
	}
	return len(append(encSourceRecord(nil, record, 0), '\n'))
}
