package protocol

// MsgType represents the application-level message identifier.
type MsgType byte

const (
	// MsgTypeEcho is a simple echo request/response.
	MsgTypeEcho MsgType = 1

	// MsgTypeMetrics is a streaming-metrics frame.
	MsgTypeMetrics MsgType = 2

	// MsgTypeCreateTopic is the control-frame sent by a client to request creation of a new topic.
	// The frame’s payload carries the topic name (and any optional configuration parameters).
	MsgTypeCreateTopic MsgType = 3

	// MsgTypeDeleteTopic is the control-frame sent by a client to request deletion of an existing topic.
	// The frame’s payload carries the name of the topic to delete.
	MsgTypeDeleteTopic MsgType = 4

	// MsgTypeDescribeTopic is the control-frame sent by a client to request metadata about a topic.
	// The frame’s payload carries the topic name; the broker responds with topic configuration data.
	MsgTypeDescribeTopic MsgType = 5

	// MsgTypePublish is sent by a producer client to publish a message to a specific topic.
	// The payload contains the topic name and the message bytes.
	MsgTypePublish MsgType = 10

	// MsgTypeSubscribe is sent by a consumer client to subscribe to a specific topic.
	// The payload contains the topic name.
	MsgTypeSubscribe MsgType = 11

	// MsgTypeMessage is sent by the broker to deliver a message to a subscribed consumer.
	// The payload contains the topic name, offset, and message bytes.
	MsgTypeMessage MsgType = 12

	// MsgTypeCommitOffset is sent by a consumer to commit its processed offset for a topic partition.
	// The payload contains the topic name and the offset being committed.
	MsgTypeCommitOffset MsgType = 13

	// MsgTypeHeartbeat is sent periodically by clients or the broker to indicate liveness.
	// The payload may be empty or include a timestamp.
	MsgTypeHeartbeat MsgType = 14

	// MsgTypeError is sent by the broker or client to signal an error condition.
	// The payload contains an error code and human-readable message.
	MsgTypeError MsgType = 15

	// MsgTypeListTopics is sent by a client to request the list of available topics.
	// The payload is empty.
	MsgTypeListTopics MsgType = 16

	// MsgTypeListTopicsResponse is sent by the broker to respond with the list of topics.
	// The payload contains a serialized list of topic names.
	MsgTypeListTopicsResponse MsgType = 17
)

// Name returns the human-readable name of the message type.
func (mt MsgType) Name() string {
	switch mt {
	case MsgTypeEcho:
		return "Echo"
	case MsgTypeMetrics:
		return "Metrics"
	case MsgTypeCreateTopic:
		return "CreateTopic"
	case MsgTypeDeleteTopic:
		return "DeleteTopic"
	case MsgTypeDescribeTopic:
		return "DescribeTopic"
	case MsgTypePublish:
		return "Publish"
	case MsgTypeSubscribe:
		return "Subscribe"
	case MsgTypeMessage:
		return "Message"
	case MsgTypeCommitOffset:
		return "CommitOffset"
	case MsgTypeHeartbeat:
		return "Heartbeat"
	case MsgTypeError:
		return "Error"
	case MsgTypeListTopics:
		return "ListTopics"
	case MsgTypeListTopicsResponse:
		return "ListTopicsResponse"
	default:
		return "Unknown"
	}
}

func (mt MsgType) Byte() byte {
	return byte(mt)
}
