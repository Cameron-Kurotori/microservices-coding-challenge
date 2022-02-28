package server

import (
	context "context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	distqueue "github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue/mock_distqueue"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/item"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFollowerSend(t *testing.T) {
	ctrl := gomock.NewController(t)
	sender := NewMockServerQueueItemSender(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	follower := &follower{
		data:   make(chan *distqueue.ServerQueueItem),
		sender: sender,
		ctx:    ctx,
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- follower.send()
	}()

	myItem := &distqueue.ServerQueueItem{
		Ts: timestamppb.Now(),
	}
	sender.EXPECT().Send(gomock.Any()).Times(1).DoAndReturn(func(item *distqueue.ServerQueueItem) error {
		assert.Equal(t, myItem.Ts.AsTime(), item.Ts.AsTime())
		return nil
	})

	follower.data <- myItem
	cancel()

	assert.Nil(t, <-errChan)
}

func TestFollowerSend_EOF(t *testing.T) {
	ctrl := gomock.NewController(t)
	sender := NewMockServerQueueItemSender(ctrl)
	follower := &follower{
		data:   make(chan *distqueue.ServerQueueItem),
		sender: sender,
		ctx:    context.Background(),
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- follower.send()
	}()

	myItem := &distqueue.ServerQueueItem{
		Ts: timestamppb.Now(),
	}
	sender.EXPECT().Send(gomock.Any()).Times(1).DoAndReturn(func(item *distqueue.ServerQueueItem) error {
		return io.EOF
	})

	follower.data <- myItem
	assert.Nil(t, <-errChan)
}

func TestFollowerSend_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	sender := NewMockServerQueueItemSender(ctrl)
	follower := &follower{
		data:   make(chan *distqueue.ServerQueueItem),
		sender: sender,
		ctx:    context.Background(),
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- follower.send()
	}()

	myItem := &distqueue.ServerQueueItem{
		Ts: timestamppb.Now(),
	}
	expectedErr := fmt.Errorf("my error")
	sender.EXPECT().Send(gomock.Any()).Times(1).DoAndReturn(func(item *distqueue.ServerQueueItem) error {
		return expectedErr
	})

	follower.data <- myItem
	assert.Equal(t, expectedErr, <-errChan)
}

func TestNew(t *testing.T) {
	server := New()
	assert.NotNil(t, server.(*distQueueServer).clients)
}

func TestReceive_EOF(t *testing.T) {
	server := New().(*distQueueServer)
	ctrl := gomock.NewController(t)
	mockReceiver := NewMockQueueReceiver(ctrl)

	mockReceiver.EXPECT().Recv().AnyTimes().DoAndReturn(func() (*distqueue.QueueItem, error) {
		return nil, io.EOF
	})
	err := server.receive(mockReceiver)
	assert.Nil(t, err)
}

func TestReceive_Error(t *testing.T) {
	server := New().(*distQueueServer)
	ctrl := gomock.NewController(t)
	mockReceiver := NewMockQueueReceiver(ctrl)

	expectedErr := status.New(codes.Canceled, "cancelled").Err()
	mockReceiver.EXPECT().Recv().AnyTimes().DoAndReturn(func() (*distqueue.QueueItem, error) {
		return nil, expectedErr
	})
	err := server.receive(mockReceiver)
	assert.Equal(t, expectedErr, err)
}

func TestReceive_ClientDoneAfter3(t *testing.T) {
	server := New().(*distQueueServer)
	ctrl := gomock.NewController(t)
	mockReceiver := NewMockQueueReceiver(ctrl)

	count := 0
	myItems := []*item.Item{
		{
			Id:        "first",
			Int:       3,
			Timestamp: timestamppb.New(time.Now()),
		},
		{
			Id:        "second",
			Int:       123,
			Timestamp: timestamppb.New(time.Now().Add(time.Second * 5)),
		},
		{
			Id:        "third",
			Int:       12512,
			Timestamp: timestamppb.New(time.Now().Add(time.Second * 10)),
		},
	}
	mockReceiver.EXPECT().Recv().AnyTimes().DoAndReturn(func() (*distqueue.QueueItem, error) {
		if count >= 3 {
			return nil, io.EOF
		}
		anyItem, _ := anypb.New(myItems[count])
		count++
		return &distqueue.QueueItem{
			Item: anyItem,
		}, nil
	})
	err := server.receive(mockReceiver)
	assert.Nil(t, err)
	assert.Equal(t, len(myItems), count)
	assert.Len(t, server.data, len(myItems))

	for i, myItem := range myItems {
		unmarshalledItem := item.Item{}
		err = server.data[i].Item.Item.UnmarshalTo(&unmarshalledItem)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, myItem.Id, unmarshalledItem.Id)
		assert.Equal(t, myItem.Int, unmarshalledItem.Int)
		assert.Equal(t, myItem.Timestamp.AsTime(), unmarshalledItem.Timestamp.AsTime())
	}

}

func TestPush(t *testing.T) {
	server := New().(*distQueueServer)

	for i := 0; i < 3; i++ {
		server.clients[fmt.Sprintf("client-%d", i)] = &follower{
			data: make(chan *distqueue.ServerQueueItem, 2),
		}
	}

	myItem := &item.Item{
		Id:        "test",
		Timestamp: timestamppb.Now(),
	}
	anyItem, _ := anypb.New(myItem)
	time.Sleep(time.Second * 3)
	pushTime := time.Now()
	server.push(&distqueue.QueueItem{Item: anyItem})
	time.Sleep(time.Second * 3)
	server.push(&distqueue.QueueItem{Item: anyItem})

	for _, client := range server.clients {
		receivedItem := <-client.data
		assert.Nil(t, receivedItem.PrevTs)
		assert.InDelta(t, 0, receivedItem.Ts.AsTime().Sub(pushTime).Milliseconds(), 50)

		receivedItem2 := <-client.data
		assert.Equal(t, receivedItem.Ts.AsTime(), receivedItem2.PrevTs.AsTime())

		unmarshalledItem := &item.Item{}
		err := receivedItem.Item.Item.UnmarshalTo(unmarshalledItem)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, myItem.Id, unmarshalledItem.Id)
		assert.Equal(t, myItem.Int, unmarshalledItem.Int)
		assert.Equal(t, myItem.Timestamp.AsTime(), unmarshalledItem.Timestamp.AsTime())
	}
}

func TestSync_SendErr(t *testing.T) {
	server := New()
	ctrl := gomock.NewController(t)
	responses := make(chan struct {
		val *distqueue.QueueItem
		err error
	}, 1)
	sendErrors := make(chan error, 1)

	firstReceive := sync.Once{}
	receiving := sync.WaitGroup{}
	receiving.Add(1)

	syncServer := mock_distqueue.NewMockDistributedQueueService_SyncServer(ctrl)
	syncServer.EXPECT().Recv().AnyTimes().DoAndReturn(func() (*distqueue.QueueItem, error) {
		firstReceive.Do(receiving.Done)
		resp := <-responses
		return resp.val, resp.err
	})

	syncServer.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(func(item *distqueue.ServerQueueItem) error {
		return <-sendErrors
	})

	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		err := server.Sync(syncServer)
		if err != nil {
			errChan <- err
		}
	}()

	receiving.Wait()

	expectedErr := fmt.Errorf("my error")
	sendErrors <- expectedErr
	responses <- struct {
		val *distqueue.QueueItem
		err error
	}{
		&distqueue.QueueItem{}, nil,
	}

	assert.Equal(t, expectedErr, <-errChan)
}

func TestSync_ReceiveErr(t *testing.T) {
	server := New()
	ctrl := gomock.NewController(t)
	responses := make(chan struct {
		val *distqueue.QueueItem
		err error
	}, 1)

	receiving := sync.WaitGroup{}
	receiving.Add(1)

	syncServer := mock_distqueue.NewMockDistributedQueueService_SyncServer(ctrl)
	syncServer.EXPECT().Recv().AnyTimes().DoAndReturn(func() (*distqueue.QueueItem, error) {
		receiving.Done()
		resp := <-responses
		return resp.val, resp.err
	})

	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		err := server.Sync(syncServer)
		if err != nil {
			errChan <- err
		}
	}()

	receiving.Wait()

	expectedErr := fmt.Errorf("my error")
	responses <- struct {
		val *distqueue.QueueItem
		err error
	}{
		nil, expectedErr,
	}

	assert.Equal(t, expectedErr, <-errChan)
}

func TestSyncAndStats(t *testing.T) {
	server := New()
	ctrl := gomock.NewController(t)
	responses := make(chan struct {
		val *distqueue.QueueItem
		err error
	}, 1)

	receiving := sync.WaitGroup{}
	receiving.Add(1)

	syncServer := mock_distqueue.NewMockDistributedQueueService_SyncServer(ctrl)
	syncServer.EXPECT().Recv().AnyTimes().DoAndReturn(func() (*distqueue.QueueItem, error) {
		receiving.Done()
		resp := <-responses
		return resp.val, resp.err
	})

	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		err := server.Sync(syncServer)
		if err != nil {
			errChan <- err
		}
	}()

	receiving.Wait()

	assert.Equal(t, 1, server.Stats().NumberClients)

	responses <- struct {
		val *distqueue.QueueItem
		err error
	}{
		nil, io.EOF,
	}

	err := <-errChan
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 0, server.Stats().NumberClients)
}
