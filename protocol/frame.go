package protocol

import "encoding/binary"

// Frame is the interface implemented by all frame types.
type Frame interface {
	Code() FrameCode
	Serialize(buffer []byte) []byte
	Deserialize(buffer []byte, err *error) []byte
	ByteCount() int
}

// CreateBundleRequest creates a bundle
type CreateBundleRequest struct {
	Bundle BundleIdent
}

// CreateBundleReply is the reply to CreateBundleRequest
type CreateBundleReply struct {
	Error ErrorCode
}

// OpenStreamRequest opens a stream
type OpenStreamRequest struct {
	Stream StreamIdent
}

// OpenStreamReply is the reply to OpenStreamRequest
type OpenStreamReply struct {
	Error ErrorCode
}

// DestRequest adds or removes a destination of the bundle.
type DestRequest struct {
	Flags DestFlags
	User  UserIdent
}

// DestReply is the reply to FrameDest
type DestReply struct {
	Error ErrorCode
}

// WriteRequest sends data to the server
type WriteRequest struct {
	Flags WriteFlags
	Data  []byte
}

// CommitNotice acknowledges a WriteRequest if WriteCommit has been used.
type CommitNotice struct {
	Error ErrorCode
	// The bundle-time at which the commit occured.
	// No two commits on the same bundle can have the same commit time.
	Time int64
}

// ReadRequest asks the server to read a byte range or a sequence of records.
// When the amount of bytes or records to read is negative, all data is read
// and ServerPush is enabled.
// When the amount of bytes or records to read is null, ServerPush is disabled.
type ReadRequest struct {
	Seek   SeekFlags
	Offset int64
	// Number of bytes to read.
	// A value of 0 means to read nothing and that ServerPush becomes disabled.
	// A value < 0 means that all data should be pushed by the server
	// once it is available.
	Count int64
}

// PushNotice is sent by the server to push new stream data to the client.
// This happens as response to ReadRequest or when a stream is opened with
// ServerPush enabled.
type PushNotice struct {
	Flags DataFlags
	Data  []byte
}

// CloseNotice closes the stream on behalf of the client.
type CloseNotice struct {
}

// ProgressNotice informs about the reading progress of other clients
// when ObserveReads has been used.
type ProgressNotice struct {
	User   UserIdent
	Offset uint64
}

// Code implements the Frame interface.
func (f *CreateBundleRequest) Code() FrameCode {
	return FrameCreateBundle
}

// Serialize write the request to a buffer
func (f *CreateBundleRequest) Serialize(buffer []byte) []byte {
	buffer = f.Bundle.Serialize(buffer)
	return buffer
}

// Deserialize reads a CreateBundleRequest from the buffer and returns
// the remaining buffer.
func (f *CreateBundleRequest) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	buffer = f.Bundle.Deserialize(buffer, err)
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *CreateBundleRequest) ByteCount() int {
	return f.Bundle.ByteCount()
}

// Code implements the Frame interface.
func (f *CreateBundleReply) Code() FrameCode {
	return FrameCreateBundle
}

// Serialize write the request to a buffer
func (f *CreateBundleReply) Serialize(buffer []byte) []byte {
	binary.LittleEndian.PutUint32(buffer, uint32(f.Error))
	return buffer
}

// Deserialize reads a CreateBundleRequest from the buffer and returns
// the remaining buffer.
func (f *CreateBundleReply) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	if len(buffer) != 4 {
		*err = errDeserialize
		return nil
	}
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *CreateBundleReply) ByteCount() int {
	return 4
}

// Code implements the Frame interface.
func (f *OpenStreamRequest) Code() FrameCode {
	return FrameOpenStream
}

// Serialize write the request to a buffer
func (f *OpenStreamRequest) Serialize(buffer []byte) []byte {
	buffer = f.Stream.Serialize(buffer)
	return buffer
}

// Deserialize reads a CreateBundleRequest from the buffer and returns
// the remaining buffer.
func (f *OpenStreamRequest) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	buffer = f.Stream.Deserialize(buffer, err)
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *OpenStreamRequest) ByteCount() int {
	return f.Stream.ByteCount()
}

// Code implements the Frame interface.
func (f *OpenStreamReply) Code() FrameCode {
	return FrameOpenStreamReply
}

// Serialize write the request to a buffer
func (f *OpenStreamReply) Serialize(buffer []byte) []byte {
	binary.LittleEndian.PutUint32(buffer, uint32(f.Error))
	return buffer
}

// Deserialize reads a CreateBundleRequest from the buffer and returns
// the remaining buffer.
func (f *OpenStreamReply) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	if len(buffer) != 4 {
		*err = errDeserialize
		return nil
	}
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *OpenStreamReply) ByteCount() int {
	return 4
}

// Code implements the Frame interface.
func (f *DestRequest) Code() FrameCode {
	return FrameDest
}

// Serialize write the request to a buffer
func (f *DestRequest) Serialize(buffer []byte) []byte {
	buffer[0] = byte(f.Flags)
	buffer = f.User.Serialize(buffer[1:])
	return buffer
}

// Deserialize reads a DestRequest from the buffer and returns
// the remaining buffer.
func (f *DestRequest) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	if len(buffer) < 1 {
		*err = errDeserialize
		return nil
	}
	f.Flags = DestFlags(buffer[0])
	buffer = f.User.Deserialize(buffer[1:], err)
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *DestRequest) ByteCount() int {
	return 1 + f.User.ByteCount()
}

// Code implements the Frame interface.
func (f *DestReply) Code() FrameCode {
	return FrameDestReply
}

// Serialize write the request to a buffer
func (f *DestReply) Serialize(buffer []byte) []byte {
	binary.LittleEndian.PutUint32(buffer, uint32(f.Error))
	return buffer
}

// Deserialize reads a DestReply from the buffer and returns
// the remaining buffer.
func (f *DestReply) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	if len(buffer) != 4 {
		*err = errDeserialize
		return nil
	}
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *DestReply) ByteCount() int {
	return 4
}

// Code implements the Frame interface.
func (f *WriteRequest) Code() FrameCode {
	return FrameWrite
}

// Serialize write the request to a buffer
func (f *WriteRequest) Serialize(buffer []byte) []byte {
	buffer[0] = byte(f.Flags)
	copy(buffer[1:], f.Data)
	return buffer
}

// Deserialize reads a WriteRequest from the buffer and returns
// the remaining buffer.
func (f *WriteRequest) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	if len(buffer) < 1 {
		*err = errDeserialize
		return nil
	}
	f.Flags = WriteFlags(buffer[0])
	f.Data = buffer[1:]
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *WriteRequest) ByteCount() int {
	return 1 + len(f.Data)
}

// Code implements the Frame interface.
func (f *CommitNotice) Code() FrameCode {
	return FrameCommit
}

// Serialize write the request to a buffer
func (f *CommitNotice) Serialize(buffer []byte) []byte {
	binary.LittleEndian.PutUint32(buffer, uint32(f.Error))
	binary.LittleEndian.PutUint64(buffer[4:], uint64(f.Time))
	return buffer
}

// Deserialize reads a CommitNotice from the buffer and returns
// the remaining buffer.
func (f *CommitNotice) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	if len(buffer) != 4+8 {
		*err = errDeserialize
		return nil
	}
	f.Error = ErrorCode(binary.LittleEndian.Uint32(buffer))
	f.Time = int64(binary.LittleEndian.Uint64(buffer[4:]))
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *CommitNotice) ByteCount() int {
	return 4 + 8
}

// Code implements the Frame interface.
func (f *ReadRequest) Code() FrameCode {
	return FrameRead
}

// Serialize write the request to a buffer
func (f *ReadRequest) Serialize(buffer []byte) []byte {
	buffer[0] = byte(f.Seek)
	binary.LittleEndian.PutUint64(buffer[1:], uint64(f.Offset))
	binary.LittleEndian.PutUint64(buffer[9:], uint64(f.Count))
	return buffer
}

// Deserialize reads a CommitNotice from the buffer and returns
// the remaining buffer.
func (f *ReadRequest) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	if len(buffer) != 1+8+8 {
		*err = errDeserialize
		return nil
	}
	f.Seek = SeekFlags(buffer[0])
	f.Offset = int64(binary.LittleEndian.Uint64(buffer[1:]))
	f.Count = int64(binary.LittleEndian.Uint64(buffer[9:]))
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *ReadRequest) ByteCount() int {
	return 1 + 8 + 8
}

// Code implements the Frame interface.
func (f *PushNotice) Code() FrameCode {
	return FrameServerPush
}

// Serialize write the request to a buffer
func (f *PushNotice) Serialize(buffer []byte) []byte {
	buffer[0] = byte(f.Flags)
	copy(buffer[1:], f.Data)
	return buffer
}

// Deserialize reads a PushNotice from the buffer and returns
// the remaining buffer.
func (f *PushNotice) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	if len(buffer) < 1 {
		*err = errDeserialize
		return nil
	}
	f.Flags = DataFlags(buffer[0])
	f.Data = buffer[1:]
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *PushNotice) ByteCount() int {
	return 1 + len(f.Data)
}

// Code implements the Frame interface.
func (f *CloseNotice) Code() FrameCode {
	return FrameClose
}

// Serialize write the request to a buffer
func (f *CloseNotice) Serialize(buffer []byte) []byte {
	return buffer
}

// Deserialize reads a CloseNotice from the buffer and returns
// the remaining buffer.
func (f *CloseNotice) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	if len(buffer) != 0 {
		*err = errDeserialize
		return nil
	}
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *CloseNotice) ByteCount() int {
	return 0
}

// Code implements the Frame interface.
func (f *ProgressNotice) Code() FrameCode {
	return FrameProgress
}

// Serialize write the request to a buffer
func (f *ProgressNotice) Serialize(buffer []byte) []byte {
	binary.LittleEndian.PutUint64(buffer, f.Offset)
	buffer = f.User.Serialize(buffer[8:])
	return buffer
}

// Deserialize reads a DestRequest from the buffer and returns
// the remaining buffer.
func (f *ProgressNotice) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	if len(buffer) < 8 {
		*err = errDeserialize
		return nil
	}
	f.Offset = binary.LittleEndian.Uint64(buffer)
	buffer = f.User.Deserialize(buffer[8:], err)
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (f *ProgressNotice) ByteCount() int {
	return 8 + f.User.ByteCount()
}

// SerializeFrame returns a byte array with the serialized frame.
func SerializeFrame(flow uint32, f Frame) []byte {
	data := make([]byte, 4+1+f.ByteCount())
	binary.LittleEndian.PutUint32(data, flow)
	data[4] = byte(f.Code())
	f.Serialize(data[5:])
	return data
}

// DeserializeFrame deserializes the flow number and the frame.
func DeserializeFrame(buffer []byte) (flow uint32, frame Frame, err error) {
	if len(buffer) < 4+1 {
		return 0, nil, errDeserialize
	}
	flow = binary.LittleEndian.Uint32(buffer)
	switch FrameCode(buffer[4]) {
	case FrameCreateBundle:
		frame = &CreateBundleRequest{}
	case FrameCreateBundleReply:
		frame = &CreateBundleReply{}
	case FrameOpenStream:
		frame = &OpenStreamRequest{}
	case FrameOpenStreamReply:
		frame = &OpenStreamReply{}
	case FrameDest:
		frame = &DestRequest{}
	case FrameDestReply:
		frame = &DestReply{}
	case FrameWrite:
		frame = &WriteRequest{}
	case FrameCommit:
		frame = &CommitNotice{}
	case FrameRead:
		frame = &ReadRequest{}
	case FrameServerPush:
		frame = &PushNotice{}
	case FrameClose:
		frame = &CloseNotice{}
	case FrameProgress:
		frame = &ProgressNotice{}
	default:
		return 0, nil, errDeserialize
	}
	frame.Deserialize(buffer[5:], &err)
	return
}
