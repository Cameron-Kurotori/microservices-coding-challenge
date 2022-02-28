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
	dq        distqueue.DistributedQueueService_SyncClient
	queueChan chan *anypb.Any
	ctx       context.Context
}

func (q *Queue) Push(item *anypb.Any) error {
	return q.dq.Send(&distqueue.QueueItem{
		Item: item,
	})
}

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
			if err != nil {
				if err == io.EOF {
					continue
				}
				fmt.Fprintf(os.Stderr, "error while receiving: %+v\n", err)
				// TODO: error handling
				return
			}

			if item != nil {
				q.queueChan <- item.Item.Item
			}
		}
	}
}

func new(dqClient distqueue.DistributedQueueServiceClient) (*Queue, func(), error) {
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

	// receive messages from the server
	go q.receive()

	return q, cancel, nil
}

func New(target string) (*Queue, func(), error) {
	grpcClient, err := grpc.NewClient(target)
	if err != nil {
		return nil, nil, err
	}
	dqClient := distqueue.NewDistributedQueueServiceClient(grpcClient)
	return new(dqClient)
}
