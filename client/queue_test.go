package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue/mock_distqueue"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/item"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNew(t *testing.T) {
	ctrl := gomock.NewController(t)

	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)
	connectClient.EXPECT().Send(gomock.Any()).AnyTimes().Return(nil)
	connectClient.EXPECT().Recv().AnyTimes().Return(nil, io.EOF)
	connectClient.EXPECT().CloseSend().Return(nil)

	dqClient.EXPECT().Connect(gomock.Any()).AnyTimes().Return(connectClient, nil)

	q, err := new(dqClient)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	q.Done()
}

func TestNew_SyncFail(t *testing.T) {
	ctrl := gomock.NewController(t)

	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)
	dqClient.EXPECT().Connect(gomock.Any()).AnyTimes().Return(nil, fmt.Errorf("failed to create sync client"))

	_, err := new(dqClient)
	if err == nil {
		t.Fatal("expected error, got none")
	}
}

func TestNew_ReceiveErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)
	expectedErr := fmt.Errorf("expected err")
	connectClient.EXPECT().Recv().AnyTimes().Return(nil, expectedErr)
	connectClient.EXPECT().CloseSend().Return(nil)

	dqClient.EXPECT().Connect(gomock.Any()).AnyTimes().Return(connectClient, nil)

	q, err := new(dqClient)
	if err != nil {
		t.Fatal(err)
	}

	err = <-q.ReceiveErrors
	assert.Equal(t, expectedErr, err)
	q.Done()
}

func TestNew_ReceiveEOF(t *testing.T) {
	ctrl := gomock.NewController(t)

	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)
	connectClient.EXPECT().Recv().AnyTimes().Return(nil, io.EOF)
	connectClient.EXPECT().CloseSend().Return(nil)

	dqClient.EXPECT().Connect(gomock.Any()).AnyTimes().Return(connectClient, nil)

	q, err := new(dqClient)
	if err != nil {
		t.Fatal(err)
	}

	err = <-q.ReceiveErrors
	assert.Nil(t, err)
	q.Done()
}

func TestReceive_CtxCancelled(t *testing.T) {
	ctrl := gomock.NewController(t)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	q := &Queue{
		dq:        connectClient,
		queueChan: make(chan *anypb.Any, 1),
		ctx:       ctx,
	}

	cancel()
	err := q.receive()
	assert.Nil(t, err)
}

func TestReceive_EOF(t *testing.T) {
	ctrl := gomock.NewController(t)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	connectClient.EXPECT().Recv().DoAndReturn(func() (*distqueue.ServerQueueItem, error) {
		cancel()
		return nil, io.EOF
	})

	q := &Queue{
		dq:        connectClient,
		queueChan: make(chan *anypb.Any, 1),
		ctx:       ctx,
	}

	err := q.receive()
	assert.Nil(t, err)
}

func TestReceive_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)

	ctx := context.Background()
	expectedErr := fmt.Errorf("receive error")

	connectClient.EXPECT().Recv().DoAndReturn(func() (*distqueue.ServerQueueItem, error) {
		return nil, expectedErr
	})
	q := &Queue{
		dq:        connectClient,
		queueChan: make(chan *anypb.Any, 1),
		ctx:       ctx,
	}

	err := q.receive()
	assert.Equal(t, expectedErr, err)
}

func TestReceive_ReceiveOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	originalItem := &item.Item{
		Id: "test",
	}
	queueItem, _ := anypb.New(originalItem)
	i := 0
	data := []*distqueue.ServerQueueItem{
		{Item: &distqueue.QueueItem{Item: queueItem}},
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	once := sync.Once{}
	receiveFn := func() (*distqueue.ServerQueueItem, error) {
		if i >= len(data) {
			once.Do(wg.Done)
			return nil, io.EOF
		}
		ret := data[i]
		i++
		return ret, nil
	}

	connectClient.EXPECT().Recv().MinTimes(2).DoAndReturn(receiveFn)

	q := &Queue{
		dq:        connectClient,
		queueChan: make(chan *anypb.Any, 1),
		ctx:       ctx,
	}

	go q.receive()

	wg.Wait()

	select {
	case queueItem, ok := <-q.queueChan:
		actual := &item.Item{}
		err := queueItem.UnmarshalTo(actual)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, originalItem.Id, actual.Id)
		assert.True(t, ok)
	default:
		t.Fatal("expected item")
	}

	cancel()
}
func TestPushPop(t *testing.T) {
	ctrl := gomock.NewController(t)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)
	var lastItemTS *timestamppb.Timestamp
	itemChan := make(chan *distqueue.ServerQueueItem, 10)
	connectClient.EXPECT().Send(gomock.Any()).Times(1).DoAndReturn(func(item *distqueue.QueueItem) error {
		now := timestamppb.Now()
		itemChan <- &distqueue.ServerQueueItem{
			Item:   item,
			PrevTs: lastItemTS,
			Ts:     timestamppb.Now(),
		}
		lastItemTS = now
		return nil
	})

	itemReceived := make(chan bool, 1)
	connectClient.EXPECT().Recv().MinTimes(1).DoAndReturn(func() (*distqueue.ServerQueueItem, error) {
		select {
		case item := <-itemChan:
			close(itemReceived)
			return item, nil
		default:
			return nil, io.EOF
		}
	})

	ctx := context.Background()
	q := &Queue{
		dq:        connectClient,
		queueChan: make(chan *anypb.Any),
		ctx:       ctx,
	}

	queueItem := &item.Item{Id: "test"}
	anyItem, _ := anypb.New(queueItem)
	err := q.Push(anyItem)
	if err != nil {
		t.Fatal(err)
	}

	nilItem := q.Pop()
	assert.Nil(t, nilItem)

	go q.receive()

	// wait until item received
	<-itemReceived

	unmarshalledItem := item.Item{}
	poppedItem := q.Pop()

	err = poppedItem.UnmarshalTo(&unmarshalledItem)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, queueItem.Id, unmarshalledItem.Id)

	nilItem = q.Pop()
	assert.Nil(t, nilItem)
}
