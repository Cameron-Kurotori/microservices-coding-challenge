package client

import (
	"fmt"
	"time"

	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type QueueOpt func(q *Queue)

func WarmStart(after *time.Time) QueueOpt {
	return func(q *Queue) {
		q.warmStart.enabled = true
		q.warmStart.after = after
	}
}

// WithReceiveHandler uses the handler chain to link behavior when the queue
// receives the next item from the server.
// Example Use Case: This can be used for notifying a worker that a new item has
// been received so processing can be done.
func WithReceiveHandler(chain ...ReceiveHandler) QueueOpt {
	return QueueOpt(func(q *Queue) {
		q.receiveHandlerChain = chain
	})
}

type ReceiveHandler func(*distqueue.ServerQueueItem, error) (*distqueue.ServerQueueItem, error)

var _ ReceiveHandler = MonitorMissing()

// MissingItemErr is an error returned by MonitorMissing ReceiveHandler. Details the expected
// previous timestamp and the actual previous timestamp. If there is a mismatch, this indicates that an item
// was likely dropped.
type MissingItemErr struct {
	expectedLastTS *timestamppb.Timestamp
	actualLastTS   *timestamppb.Timestamp
}

// Error implements error interface
func (err MissingItemErr) Error() string {
	return fmt.Sprintf("missing item: expected_last_timestamp=%s actual_last_timestamp=%s", err.expectedLastTS.AsTime().Format(time.RFC3339Nano), err.actualLastTS.AsTime().Format(time.RFC3339Nano))
}

// MonitorMissing is a ReceiveHandler that will monitor that the server determined order is consistent
// and no items have dropped. If potential missing item, returns MissingItemErr error. MonitorMissing
// must come before any ReceiveHandlers that drop items in the chain of handlers.
func MonitorMissing() ReceiveHandler {
	var prevTimestamp *timestamppb.Timestamp
	return func(item *distqueue.ServerQueueItem, err error) (*distqueue.ServerQueueItem, error) {
		if err == nil && prevTimestamp != nil {
			if prevTimestamp.AsTime() != item.PrevTs.AsTime() {
				return item, MissingItemErr{
					expectedLastTS: prevTimestamp,
					actualLastTS:   item.PrevTs,
				}
			}
		}
		if item != nil {
			prevTimestamp = item.Ts
		}
		return item, err
	}
}
