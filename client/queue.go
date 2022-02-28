package client

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/Cameron-Kurotori/microservices-coding-challenge/grpc"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue"
	"google.golang.org/protobuf/types/known/anypb"
)

type Queue struct {
	dq                  distqueue.DistributedQueueService_SyncClient
	queueChan           chan *anypb.Any
	ctx                 context.Context
	receiveHandlerChain []ReceiveHandler
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

func (q *Queue) receive() {
	for {
		select {
		case <-q.ctx.Done():
			close(q.queueChan)
			return
		default:
			item, err := q.dq.Recv()
			for _, handler := range q.receiveHandlerChain {
				item, err = handler(item, err)
			}
			if err != nil {
				if err == io.EOF {
					continue
				}
				fmt.Fprintf(os.Stderr, "error while receiving: %+v\n", err)
				return
			}

			if item != nil && item.Item != nil {
				q.queueChan <- item.Item.Item
			} else {
				fmt.Fprintf(os.Stderr, "item empty: skipping\n")
			}
		}
	}
}

func new(dqClient distqueue.DistributedQueueServiceClient, opts ...QueueOpt) (*Queue, func(), error) {
	dq, err := dqClient.Sync(context.Background())
	if err != nil {
		return nil, nil, err
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	cancel := func() {
		_ = dq.CloseSend()
		fmt.Fprintf(os.Stderr, "cancelling client to server\n")
		cancelCtx()
	}

	q := &Queue{
		dq:        dq,
		queueChan: make(chan *anypb.Any, 5),
		ctx:       ctx,
	}

	for _, opt := range opts {
		opt(q)
	}

	// receive messages from the server
	go q.receive()

	return q, cancel, nil
}

// New returns a new distributed queue client. When pushing items on a distributed queue,
// they are pushed to the leader that aggregates all pushes and streams back the consistent
// order to all clients. You can only receive items from the moment the client is set up.
// A cancel function is also returned which should be called when the client is no longer needed.
func New(target string, opts ...QueueOpt) (queue *Queue, cancel func(), err error) {
	grpcClient, err := grpc.NewClient(target)
	if err != nil {
		return nil, nil, err
	}
	dqClient := distqueue.NewDistributedQueueServiceClient(grpcClient)
	return new(dqClient, opts...)
}
