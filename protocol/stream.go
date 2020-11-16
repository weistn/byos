package protocol

// StreamIdent identifies a stream.
//
// <bundle>/<usr>/<name>
type StreamIdent struct {
	Bundle BundleIdent
	// The user writing to the stream.
	// This is not necessarily the user who owns the bundle.
	User UserIdent
	// The name of the stream.
	// This name must be unique among all streams of the same user in the same bundle.
	// At most 256byte long excluding the trailing zero.
	Name string
}

// Serialize writes the StreamIdent to the buffer and returns
// the amount the extended buffer.
// The buffer must have sufficient capacity.
func (s *StreamIdent) Serialize(buffer []byte) []byte {
	buffer = s.Bundle.Serialize(buffer)
	buffer = s.User.Serialize(buffer)
	buffer = serializeString(s.Name, buffer)
	return buffer
}

// Deserialize reads a StreamIdent from the buffer and returns
// the remaining buffer.
func (s *StreamIdent) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	buffer = s.Bundle.Deserialize(buffer, err)
	buffer = s.User.Deserialize(buffer, err)
	s.Name, buffer = deserializeString(buffer, err)
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (s *StreamIdent) ByteCount() int {
	return len(s.Name) + 1 + s.Bundle.ByteCount() + s.User.ByteCount()
}

// String returns the string representation of the StreamIdent.
func (s *StreamIdent) String() string {
	return s.Bundle.String() + "/" + s.User.String() + "/" + s.Name
}
