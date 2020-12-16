package util

// Span describes the size and position of a range of bytes
type Span struct {
	// First byte in the span
	From uint64
	// First byte not belonging to the span
	To uint64
}

// HasOverlap ...
func (s Span) HasOverlap(s2 Span) bool {
	return s2.To > s.From && s.To > s2.From
}

// Intersect ...
func (s Span) Intersect(s2 Span) Span {
	// No overlap
	if s2.To <= s.From || s.To <= s2.From {
		return Span{}
	}
	return Span{max(s.From, s2.From), min(s.To, s2.To)}
}

// Size ...
func (s Span) Size() uint64 {
	if s.From >= s.To {
		return 0
	}
	return s.To - s.From
}

// IsEmpty ...
func (s Span) IsEmpty() bool {
	return s.From >= s.To
}

// IsZero ...
func (s Span) IsZero() bool {
	return s.From == 0 && s.To == 0
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
