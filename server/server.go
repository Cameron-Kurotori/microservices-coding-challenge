package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ServerQueueItemSender interface {
	Send(*distqueue.ServerQueueItem) error
}

type follower struct {
	data   chan *distqueue.ServerQueueItem
	sender ServerQueueItemSender
	ctx    context.Context
}

func (f *follower) send() error {
	for {
		select {
		case <-f.ctx.Done():
			return nil
		case item := <-f.data:
			err := f.sender.Send(item)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
	}
}

type distQueueServer struct {
	queueLock  sync.RWMutex
	data       []*distqueue.ServerQueueItem
	clients    map[string]*follower
	clientLock sync.Mutex
	prevTS     *timestamppb.Timestamp
}

func (server *distQueueServer) Stats() stats {
	server.clientLock.Lock()
	defer server.clientLock.Unlock()
	return stats{
		NumberClients: len(server.clients),
	}
}

func (server *distQueueServer) GetRange(request *distqueue.GetRangeRequest, stream distqueue.DistributedQueueService_GetRangeServer) error {
	server.queueLock.RLock()
	start := 0
	end := len(server.data)

	if request.Before != nil && request.After != nil && request.After.AsTime().After(request.Before.AsTime()) {
		return status.New(codes.InvalidArgument, "After must be a valid time after Before").Err()
	}

	if request.Before != nil {
		end = sort.Search(end, func(i int) bool {
			return server.data[len(server.data)-i-1].Ts.AsTime().Before(request.Before.AsTime())
		})
		end = len(server.data) - end
	}

	if request.After != nil {
		start = sort.Search(end, func(i int) bool {
			return server.data[i].Ts.AsTime().After(request.After.AsTime())
		})
	}

	dataToSend := make(chan *distqueue.ServerQueueItem, end-start)
	for _, item := range server.data[start:end] {
		dataToSend <- item
	}
	close(dataToSend)
	server.queueLock.RUnlock()

	for item := range dataToSend {
		if err := stream.Send(item); err != nil {
			return err
		}
	}
	return nil

}

func (server *distQueueServer) Connect(stream distqueue.DistributedQueueService_ConnectServer) error {
	clientID := uuid.New().String()

	ctx, cancelCtx := context.WithCancel(context.Background())
	f := &follower{
		data:   make(chan *distqueue.ServerQueueItem, 1),
		sender: stream,
		ctx:    ctx,
	}
	errChan := make(chan error, 1)
	go func() {
		err := f.send()
		if err != nil {
			errChan <- err
		}
	}()

	server.clientLock.Lock()
	server.clients[clientID] = f
	server.clientLock.Unlock()
	cancelFunc := func() {
		fmt.Fprintf(os.Stderr, "cancelling client\n")
		cancelCtx()
		server.clientLock.Lock()
		defer server.clientLock.Unlock()
		delete(server.clients, clientID)
	}

	defer cancelFunc()

	go func() {
		err := server.receive(stream)
		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	return <-errChan
}

func (server *distQueueServer) push(item *distqueue.QueueItem) {
	server.queueLock.Lock()
	defer server.queueLock.Unlock()

	serverItem := &distqueue.ServerQueueItem{
		Item:   item,
		Ts:     timestamppb.New(time.Now()),
		PrevTs: server.prevTS,
	}
	server.prevTS = serverItem.Ts

	server.data = append(server.data, serverItem)
	server.clientLock.Lock()
	defer server.clientLock.Unlock()
	for _, client := range server.clients {
		client.data <- serverItem
	}
}

type QueueReceiver interface {
	Recv() (*distqueue.QueueItem, error)
	Context() context.Context
}

func (server *distQueueServer) receive(receiver QueueReceiver) error {
	for {
		item, err := receiver.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			fmt.Fprintf(os.Stderr, "error while receiving... cancelling: %+v\n", err)
			return err
		}
		fmt.Fprintf(os.Stderr, "server received message... %+v\n", item)
		server.push(item)
	}
}

type stats struct {
	NumberClients int
}

type Server interface {
	distqueue.DistributedQueueServiceServer
	Stats() stats
}

type backwardsCompatibleServer struct {
	distqueue.UnimplementedDistributedQueueServiceServer
	dqServer *distQueueServer
}

func (server *backwardsCompatibleServer) Stats() stats {
	return server.dqServer.Stats()
}

// Connect initializes a connection between the workers and leader. Workers stream items
// to the leader and the leader will determine the order of the items
// and send it to all connected workers. Workers will only receive messages inserted into
// the queue after Connect is called.
func (server *backwardsCompatibleServer) Connect(stream distqueue.DistributedQueueService_ConnectServer) error {
	return server.dqServer.Connect(stream)
}

// GetRange returns a stream of (in order) items between the given time range.
func (server *backwardsCompatibleServer) GetRange(request *distqueue.GetRangeRequest, stream distqueue.DistributedQueueService_GetRangeServer) error {
	return server.dqServer.GetRange(request, stream)
}

func New() Server {
	server := &distQueueServer{
		clients: make(map[string]*follower),
	}
	return &backwardsCompatibleServer{
		dqServer: server,
	}
}
