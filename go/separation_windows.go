package logging

// Separation is always SeparationNone on Windows, and that is an admission
// rather than a policy.
//
// Windows enforces file access with ACLs, and os.FileMode cannot see them: Go
// synthesises 0666 or 0444 from the read-only attribute and nothing else. So the
// mode check the Unix build uses would be answering a question about a number
// Windows never set.
//
// Reporting "none" is the safe reading. Anything that makes a decision on
// separation refuses to assume, and a caller that needs a real answer here has
// to ask the Windows security API — which is exactly the shape of the rest of
// this: the operating system operates identity, and where we cannot reach its
// answer we say so rather than inventing one.
func (s *FileSink) Separation() Separation { return SeparationNone }
