package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Cameron-Kurotori/microservices-coding-challenge/grpc"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Queue struct {
	dq                  distqueue.DistributedQueueService_ConnectClient
	queueChan           chan *anypb.Any
	ctx                 context.Context
	receiveHandlerChain []ReceiveHandler
	ReceiveErrors       chan error
	warmStart           struct {
		enabled bool
		after   *time.Time
		items   chan *distqueue.ServerQueueItem
		err     error
		errLock sync.Mutex
	}
}

// Push an item to the distributed queue
func (q *Queue) Push(item *anypb.Any) error {
	return q.dq.Send(&distqueue.QueueItem{
		Item: item,
	})
}

// Pop an item from the distributed queue
// Returns nil if queue is currently empty
func (q *Queue) Pop() (*anypb.Any, error) {
	select {
	case item, ok := <-q.queueChan:
		if !ok {
			return item, io.EOF
		}
		return item, nil
	default:
		return nil, nil
	}
}

func (q *Queue) processReceive(item *distqueue.ServerQueueItem, err error) error {
	for _, handler := range q.receiveHandlerChain {
		item, err = handler(item, err)
	}
	if err != nil {
		return err
	}

	if item != nil && item.Item != nil {
		q.queueChan <- item.Item.Item
	} else {
		q.queueChan <- nil
	}
	return nil
}

func (q *Queue) receive() error {
	var lastTS *time.Time
	if q.warmStart.items != nil {
		// first go through all warm start items
		// when the warm start go process is done, the channel will be closed
		// and we can move on to receiving from regular stream
		for warmItem := range q.warmStart.items {
			err := q.processReceive(warmItem, nil)
			if err != nil {
				return err
			}
			ts := warmItem.Ts.AsTime()
			lastTS = &ts
		}
		q.warmStart.errLock.Lock()
		err := q.warmStart.err
		q.warmStart.errLock.Unlock()
		if err != nil {
			return err
		}
	}
	for {
		select {
		case <-q.ctx.Done():
			return nil
		default:
			item, err := q.dq.Recv()
			// conditional only for warm start
			if lastTS != nil && err == nil {
				// skip received messages that are duplicate (ts is unique and in order)
				if !item.Ts.AsTime().After(*lastTS) {
					continue
				}
				lastTS = nil // set to nil so we don't fall into this conditional again
			}
			err = q.processReceive(item, err)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}

		}
	}
}

func (q *Queue) doWarmStart(dqClient distqueue.DistributedQueueServiceClient) error {
	req := &distqueue.GetRangeRequest{}
	if q.warmStart.after != nil {
		req.After = timestamppb.New(*q.warmStart.after)
	}
	stream, err := dqClient.GetRange(context.Background(), req)
	if err != nil {
		return err
	}

	q.warmStart.items = make(chan *distqueue.ServerQueueItem, 10)

	go func() {
		// close the channel once the GetRange stream is done
		// which we expect since GetRange is not a long
		// running stream.
		defer close(q.warmStart.items)
		for {
			item, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					q.warmStart.errLock.Lock()
					q.warmStart.err = err
					q.warmStart.errLock.Unlock()
				}
				return
			}
			q.warmStart.items <- item
		}
	}()

	return nil
}

func new(dqClient distqueue.DistributedQueueServiceClient, opts ...QueueOpt) (*Queue, func(), error) {
	dq, err := dqClient.Connect(context.Background())
	if err != nil {
		return nil, nil, err
	}
	queueChan := make(chan *anypb.Any, 5)
	ctx, cancelCtx := context.WithCancel(context.Background())
	cancel := func() {
		_ = dq.CloseSend()
		fmt.Fprintf(os.Stderr, "cancelling client to server\n")
		cancelCtx()
	}

	q := &Queue{
		dq:        dq,
		queueChan: queueChan,
		ctx:       ctx,
	}

	for _, opt := range opts {
		opt(q)
	}

	if q.warmStart.enabled {
		err := q.doWarmStart(dqClient)
		if err != nil {
			cancel()
			return nil, nil, err
		}
	}

	receiveErrChan := make(chan error, 1)
	q.ReceiveErrors = receiveErrChan

	// start goroutine to receive messages from the server in the background
	go func() {
		err := q.receive()
		if err != nil {
			receiveErrChan <- err
		}
		close(receiveErrChan)
		close(queueChan)
	}()

	return q, cancel, nil
}

// New returns a new distributed queue client. When pushing items on a distributed queue,
// they are pushed to the leader that aggregates all pushes and streams back the consistent
// order to all clients. You can only receive items from the moment the client is set up.
// The Done function for the queue should be called when the client is no longer needed.
// The ReceiveErrors channel should be listened on to close the client when an error is reported.
func New(target string, opts ...QueueOpt) (queue *Queue, done func(), err error) {
	grpcClient, err := grpc.NewClient(target)
	if err != nil {
		return nil, nil, err
	}
	dqClient := distqueue.NewDistributedQueueServiceClient(grpcClient)
	return new(dqClient, opts...)
}
