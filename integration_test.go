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

type testClient struct {
	*client.Queue
	done func()
	id   string
}

// numClients - number of clients to setup
// clientWait - function to call immediately when the client is finished
// clientCancelled - function to call after client has been cancelled
func setupClients(t *testing.T, numClients int, clientFinished func(), clientCancelled func()) []testClient {
	clients := []testClient{}

	// setup all the clients first for this test
	for i := 0; i < numClients; i++ {
		t.Logf("setting up client: %d", i)
		c, clientCancel, err := client.New(addr, client.WithReceiveHandler(client.MonitorMissing()))
		if err != nil {
			panic(err)
		}
		clients = append(clients, testClient{
			id:    fmt.Sprintf("client-%d", i),
			Queue: c,
			done: func() {
				clientFinished()
				t.Log("Cancelling client")
				clientCancel()
				time.Sleep(time.Second * 1) // give enough time for client to actually cancel
				clientCancelled()
			},
		})
	}
	return clients
}

// totalNumClients - number of clients in total
// numToPush - number of items to push to leader queue
// testClient - the test client to test with
func runClient(t *testing.T, totalNumClients int, numToPush int, testClient testClient) []*item.Item {
	// push items
	for itemIndex := 0; itemIndex < numToPush; itemIndex++ {
		id := fmt.Sprintf("%s-item-%d", testClient.id, itemIndex)
		queueItem, _ := anypb.New(&item.Item{
			Id: id,
		})
		err := testClient.Push(queueItem)
		if err != nil {
			panic(err)
		}
	}

	items := []*item.Item{}
	// pop from the queue client until we have the expected number of items
	for {
		queueItem := testClient.Pop()
		if queueItem == nil {
			continue
		}

		t.Log("item popped")

		unmarshalled := &item.Item{}
		_ = queueItem.UnmarshalTo(unmarshalled)
		items = append(items, unmarshalled)
		if len(items) >= numToPush*totalNumClients {
			break
		}
	}

	assert.Len(t, items, numToPush*totalNumClients)
	return items
}

// assertIdenticalItems asserts that all the list of items in the channel are exactly the same
func assertIdenticalItems(t *testing.T, allItems chan []*item.Item) {
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
}

// TestIntegration sets up a server and 3 clients
// Tests pushing and popping on the 3 clients.
// All clients are setup at start (before any pushes)
// so at end of process all should have identical queues to pop from.
func TestIntegration(t *testing.T) {
	dqServer := setupServer(t)
	numClients := 3

	// sync magic for making sure all parts run as intended
	clientWait := sync.WaitGroup{}
	clientWait.Add(numClients)
	cancelWait := sync.WaitGroup{}
	cancelWait.Add(numClients)

	clients := setupClients(t, numClients, clientWait.Done, cancelWait.Done)
	allItems := make(chan []*item.Item, numClients)
	for _, c := range clients {
		go func(c testClient) {
			defer c.done()
			items := runClient(t, numClients, 3, c)
			allItems <- items
			t.Logf("%s: items=%v", c.id, items)
		}(c)
	}

	clientWait.Wait()
	close(allItems)

	assertIdenticalItems(t, allItems)

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
