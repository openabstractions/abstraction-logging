package logging

import (
	"bufio"
	"io"
)

// lineReader reads newline-framed records.
//
// bufio.Scanner would be shorter and would silently drop any line longer than
// its buffer, which is the one failure a log reader must not have: the giant
// line is the interesting one. bufio.Reader grows instead.
type lineReader struct{ r *bufio.Reader }

func newLineReader(r io.Reader) *lineReader {
	return &lineReader{r: bufio.NewReader(r)}
}

func (l *lineReader) next() ([]byte, error) {
	line, err := l.r.ReadBytes('\n')
	if len(line) > 0 {
		return line, nil
	}
	return nil, err
}
