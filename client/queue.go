package client

import (
	"context"
	"fmt"
	"io"
	"os"
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
	Done                func()
	ReceiveErrors       chan error
	warmStart           struct {
		enabled bool
		after   *time.Time
		items   chan *distqueue.ServerQueueItem
		err     error
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
func (q *Queue) Pop() *anypb.Any {
	select {
	case item := <-q.queueChan:
		return item
	default:
		return nil
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
		fmt.Fprintf(os.Stderr, "item empty: skipping\n")
	}
	return nil
}

func (q *Queue) receive() error {
	var lastTS *time.Time
	if q.warmStart.items != nil {
		if q.warmStart.err != nil {
			return q.warmStart.err
		}
		for warmItem := range q.warmStart.items {
			err := q.processReceive(warmItem, nil)
			if err != nil {
				return err
			}
			ts := warmItem.Ts.AsTime()
			lastTS = &ts
		}
	}
	for {
		select {
		case <-q.ctx.Done():
			return nil
		default:
			item, err := q.dq.Recv()
			if lastTS != nil && err == nil {
				// skip received messages that are duplicate (ts is unique and in order)
				if !item.Ts.AsTime().After(*lastTS) {
					continue
				}
				lastTS = nil
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
	if !q.warmStart.enabled {
		return nil
	}

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
		defer close(q.warmStart.items)
		for {
			item, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					q.warmStart.err = err
				}
				return
			}
			q.warmStart.items <- item
		}
	}()

	return nil
}

func new(dqClient distqueue.DistributedQueueServiceClient, opts ...QueueOpt) (*Queue, error) {
	dq, err := dqClient.Connect(context.Background())
	if err != nil {
		return nil, err
	}
	queueChan := make(chan *anypb.Any, 5)
	ctx, cancelCtx := context.WithCancel(context.Background())
	cancel := func() {
		_ = dq.CloseSend()
		close(queueChan)
		fmt.Fprintf(os.Stderr, "cancelling client to server\n")
		cancelCtx()
	}

	q := &Queue{
		dq:        dq,
		queueChan: queueChan,
		ctx:       ctx,
		Done:      cancel,
	}

	for _, opt := range opts {
		opt(q)
	}
	err = q.doWarmStart(dqClient)
	if err != nil {
		cancel()
		return nil, err
	}

	receiveErrChan := make(chan error, 1)
	q.ReceiveErrors = receiveErrChan
	// receive messages from the server
	go func() {
		err := q.receive()
		if err != nil {
			receiveErrChan <- err
		}
		close(receiveErrChan)
	}()

	return q, nil
}

// New returns a new distributed queue client. When pushing items on a distributed queue,
// they are pushed to the leader that aggregates all pushes and streams back the consistent
// order to all clients. You can only receive items from the moment the client is set up.
// The Done function for the queue should be called when the client is no longer needed.
// The ReceiveErrors channel should be listened on to close the client when an error is reported.
func New(target string, opts ...QueueOpt) (queue *Queue, err error) {
	grpcClient, err := grpc.NewClient(target)
	if err != nil {
		return nil, err
	}
	dqClient := distqueue.NewDistributedQueueServiceClient(grpcClient)
	return new(dqClient, opts...)
}
