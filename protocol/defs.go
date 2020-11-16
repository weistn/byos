package protocol

// FrameCode denotes the kind of frame sent via the protocol.
type FrameCode byte

const (
	// FrameCreateBundle creates a bundle
	FrameCreateBundle FrameCode = 1 + iota
	// FrameCreateBundleReply is the reply to FrameCreateBundle
	FrameCreateBundleReply
	// FrameOpenStream opens a stream
	FrameOpenStream
	// FrameOpenStreamReply is the reply to FrameOpenStream
	FrameOpenStreamReply
	// FrameDest adds or removes a destination of the bundle.
	FrameDest
	// FrameDestReply is the reply to FrameDest
	FrameDestReply
	// FrameWrite sends data to the server
	FrameWrite
	// FrameCommit acknowledges a FrameWrite if WriteCommit has been used.
	FrameCommit
	// FrameRead asks the server to read a byte range or a sequence of records.
	// When the amount of bytes or records to read is negative, all data is read
	// and ServerPush is enabled.
	// When the amount of bytes or records to read is null, ServerPush is disabled.
	FrameRead
	// FrameServerPush asks the server to push new stream data automatically
	FrameServerPush
	// FrameClose closes a frame
	FrameClose
	// FrameProgress informs about the reading progress of other clients
	// when ObserveReads has been used.
	FrameProgress
)

// StreamMode denotes the kind of stream.
type StreamMode uint32

const (
	// NormalStream is the default value.
	// Writing to the stream appends data.
	// A commit forces the server to commit data to stable storage.
	// Concurrent writes are not allowed.
	NormalStream StreamMode = iota
	// ImmutableStream means that the stream is written once, followed by a final commit.
	// Further writes are impossible.
	// Concurrent writes are not allowed.
	ImmutableStream
	// TransientStream means that the stream is subject to automatic pollard
	// once all destinations have read the stream up to a point.
	// Concurrent writes are not allowed.
	TransientStream
	// LiveStream means that data is not persisted. It is pushed to all destinations.
	// In case of congestion, data may be dropped.
	// Concurrent writes are not allowed.
	LiveStream
	// RecordStream means that data is written in susequent records.
	// A commit denotes the end of a record.
	// Concurrent writes are supported.
	RecordStream
	// TransientRecordStream means that data is written in records.
	// A commit denotes the end of a record.
	// The stream is subject to automatic pollard
	// once all destinations have read the stream up to a point.
	// Pollard will remove entire records only.
	// Concurrent writes are supported.
	TransientRecordStream
	// PublicStream means that the stream is publicly accessible for reading.
	PublicStream StreamMode = 1 << 7
)

// StreamOpenFlags are used when opening or creating a stream.
type StreamOpenFlags uint32

const (
	// ObserveReads means that the client is informed
	// when destinations have advances with reading the stream.
	ObserveReads StreamOpenFlags = 1 << (8 + iota)
	// ServerPush means that the server should push any readable data as fast as possible while
	// the client will not send additional read requests.
	// This is useful for the frequent scenario of reading the entire stream.
	// Using this flag is a shortcut. The other option is to open the stream normally
	// and to send a FrameRead afterwards.
	ServerPush
	// RandomAccess means that the stream allows for seek & read.
	// This cannot be used on live streams and destinations cannot use it on transient streams.
	// A combination with ServerPush is not possible either.
	RandomAccess
	// CreateStream creates the stream if it does not exist.
	CreateStream
	// ExclusiveStream creates the stream and fails if it does already exist.
	ExclusiveStream
	// ReadGateway means that the stream is opened on the gateway no matter whether it is a remote or not.
	// The remote server will not be contacted, unless ReadThrough is specified in addition.
	ReadGateway
	// ReadThrough means that the stream is opened via the gateway and the gateway persists all data read from the stream.
	ReadThrough = ReadGateway | (1 << 15)
)

// BundleOpenFlags is used when creating bundles.
// These flags can be or'ed with StreamOpenFlags and StreamMode
type BundleOpenFlags uint32

const (
	// TruncateBundle means that If the bundle exists, it is truncated and gets a new incarnation id.
	TruncateBundle BundleOpenFlags = 1 << (16 + iota)
	// ExclusiveBundle means that if the bundle exists, an error is reported and the existing bundle is not modified.
	ExclusiveBundle
)

// SeekFlags is used for seek operations inside of streams when RandomAccess
// has been specified while opening the stream.
type SeekFlags byte

const (
	// SeekCurrent instructs ths server not to change the position in the stream.
	SeekCurrent SeekFlags = 1 + iota
	// SeekTop seeks relative to the first (available) byte or record in the stream.
	SeekTop
	// SeekLatest seeks relative to the last byte or record in the stream
	SeekLatest
)

// DestFlags configure the state of a bundle destination.
type DestFlags byte

const (
	// DestActive means that the destination should have access to the bundle.
	DestActive = 1
	// DestInactive means that the destination is not active anymore, i.e. no data is pushed there and reads are not allowed either.
	DestInactive = 2
	// DestIncognito means that the destination is not visible to other users except for the one who created the stream.
	DestIncognito = 4
)

// WriteFlags is used together with FrameWrite.
type WriteFlags byte

const (
	// CommitWrite asks the server to send a FrameCommit after writing
	// to stable storage has completed.
	CommitWrite WriteFlags = 1 << iota
	// CloseRecord denotes that this write finishes writing a record.
	// This is only possible for stream of the kind Record or TransientRecord.
	CloseRecord
)

// DataFlags are used by PushNotice to specify additional
// meta data.
type DataFlags byte

const (
	// EndOfRecord means that the transfered data is either an entire
	// record, or the tail of a record.
	EndOfRecord DataFlags = 1 << iota
	// NewRead means the data pushed is the first on behalf of a ReadRequest has this flag set.
	// This allows to match sequences of PushNotices to the corresponding ReadRequests.
	NewRead
)

// ErrorCode is transmitted by reply frames
type ErrorCode uint32
