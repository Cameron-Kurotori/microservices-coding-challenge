package main

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Cameron-Kurotori/microservices-coding-challenge/client"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/grpc"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/item"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/server"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	addr = ":3787"
)

// setupServer sets up a distributed queue server
func setupServer(t *testing.T) server.Server {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()

	dqServer := server.New()
	distqueue.RegisterDistributedQueueServiceServer(grpcServer, dqServer)

	go func() {
		err = grpcServer.Serve(ln)
		if err != nil {
			t.Logf("error from server: %+v", err)
		}
	}()

	return dqServer
}

// TestIntegration sets up a server and 3 clients
// Tests pushing and popping on the 3 clients.
// All clients are setup at start (before any pushes)
// so at end of process all should have identical queues to pop from.
func TestIntegration(t *testing.T) {
	dqServer := setupServer(t)
	numClients := 3
	clientWait := sync.WaitGroup{}
	clientWait.Add(numClients)

	cancelWait := sync.WaitGroup{}
	cancelWait.Add(numClients)

	type testClient struct {
		*client.Queue
		cancel func()
	}

	clients := []testClient{}

	// setup all the clients first for this test
	for i := 0; i < numClients; i++ {
		t.Logf("setting up client: %d", i)
		c, clientCancel, err := client.New(addr)
		if err != nil {
			panic(err)
		}
		clients = append(clients, testClient{
			Queue: c,
			cancel: func() {
				clientWait.Done()
				t.Log("Cancelling client")
				clientCancel()
				time.Sleep(time.Second * 1) // give enough time for client to actually cancel
				cancelWait.Done()
			},
		})
	}

	allItems := make(chan []*item.Item, numClients)
	numToAdd := 3 // number of items to add per client

	for i, c := range clients {
		go func(i int, c testClient) {
			// once this flow is done, the client is finished
			defer c.cancel()

			// push items
			for j := 0; j < numToAdd; j++ {
				id := fmt.Sprintf("client-%d,item-%d", i, j)
				queueItem, _ := anypb.New(&item.Item{
					Id: id,
				})
				err := c.Push(queueItem)
				if err != nil {
					panic(err)
				}
			}

			items := []*item.Item{}
			// pop from the queue client until we have the expected number of items
			for {
				queueItem := c.Pop()
				if queueItem == nil {
					continue
				}

				t.Log("item popped")

				unmarshalled := &item.Item{}
				_ = queueItem.UnmarshalTo(unmarshalled)
				items = append(items, unmarshalled)
				if len(items) >= numToAdd*numClients {
					break
				}
			}

			assert.Len(t, items, numToAdd*numClients)
			allItems <- items
			t.Logf("client=%d items=%v", i, items)
		}(i, c)
	}

	clientWait.Wait()
	close(allItems)

	// check to make sure all queue clients have identical order
	var first []*item.Item
	for items := range allItems {
		if first == nil {
			first = items
		}
		assert.Equal(t, len(first), len(items))
		for i, item := range items {
			assert.Equal(t, first[i], item)
		}
	}

	cancelDone := make(chan bool)
	go func() {
		cancelWait.Wait()
		close(cancelDone)
	}()

	select {
	case <-cancelDone:
		assert.Equal(t, 0, dqServer.Stats().NumberClients)
	case <-time.After(time.Second * 5):
		t.Fatal("failed to close clients on server")
	}
}
