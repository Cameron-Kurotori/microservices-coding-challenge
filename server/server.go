package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue"
	"github.com/google/uuid"
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
	distqueue.UnimplementedDistributedQueueServiceServer
	prevTS *timestamppb.Timestamp
}

func (server *distQueueServer) Stats() stats {
	server.clientLock.Lock()
	defer server.clientLock.Unlock()
	return stats{
		NumberClients: len(server.clients),
	}
}

func (server *distQueueServer) Sync(stream distqueue.DistributedQueueService_ConnectServer) error {
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

func New() Server {
	server := &distQueueServer{
		clients: make(map[string]*follower),
	}
	return server
}
