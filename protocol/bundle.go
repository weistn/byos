package protocol

// BundleIdent identifies a bundle.
//
// <app>/<usr>/<name+incarnation>
//
// The string is encoded as UTF-8.
type BundleIdent struct {
	// At most 256byte long excluding the trailing zero.
	App string
	// The user who created the bundle.
	User UserIdent
	// At most 256byte long excluding the trailing zero.
	Name string
	// At most 64byte long excluding the trailing zero.
	Incarnation string
}

// Serialize writes the BundleIdent to the buffer and returns
// the amount the extended buffer.
// The buffer must have sufficient capacity.
func (b *BundleIdent) Serialize(buffer []byte) []byte {
	buffer = serializeString(b.App, buffer)
	buffer = b.User.Serialize(buffer)
	buffer = serializeString(b.Name, buffer)
	buffer = serializeString(b.Incarnation, buffer)
	return buffer
}

// Deserialize reads a BundleIdent from the buffer and returns
// the remaining buffer.
func (b *BundleIdent) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	b.App, buffer = deserializeString(buffer, err)
	buffer = b.User.Deserialize(buffer, err)
	b.Name, buffer = deserializeString(buffer, err)
	b.Incarnation, buffer = deserializeString(buffer, err)
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (b *BundleIdent) ByteCount() int {
	return len(b.App) + 1 + len(b.Name) + 1 + len(b.Incarnation) + 1 + b.User.ByteCount()
}

// String returns the string representation of the BundleIdent.
func (b *BundleIdent) String() string {
	str := b.App + "/" + b.User.String() + "/" + b.Name
	if b.Incarnation != "" {
		str += "+" + b.Incarnation
	}
	return str
}
