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
	"google.golang.org/grpc"
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

	_, done, err := new(dqClient)
	if err != nil {
		t.Fatal(err)
	}
	defer done()

	time.Sleep(100 * time.Millisecond)
}

func TestNew_SyncFail(t *testing.T) {
	ctrl := gomock.NewController(t)

	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)
	dqClient.EXPECT().Connect(gomock.Any()).AnyTimes().Return(nil, fmt.Errorf("failed to create sync client"))

	_, _, err := new(dqClient)
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

	q, done, err := new(dqClient)
	if err != nil {
		t.Fatal(err)
	}
	defer done()

	err = <-q.ReceiveErrors
	assert.Equal(t, expectedErr, err)
}

func TestNew_ReceiveEOF(t *testing.T) {
	ctrl := gomock.NewController(t)

	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)
	connectClient.EXPECT().Recv().AnyTimes().Return(nil, io.EOF)
	connectClient.EXPECT().CloseSend().Return(nil)

	dqClient.EXPECT().Connect(gomock.Any()).AnyTimes().Return(connectClient, nil)

	q, done, err := new(dqClient)
	if err != nil {
		t.Fatal(err)
	}
	defer done()

	err = <-q.ReceiveErrors
	assert.Nil(t, err)
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

func TestReceive_WarmStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)

	q := &Queue{
		dq:        connectClient,
		queueChan: make(chan *anypb.Any, 1),
		ctx:       context.Background(),
	}

	warmItems := []*distqueue.ServerQueueItem{}
	var lastTS *timestamppb.Timestamp
	for i := 0; i < 3; i++ {
		now := timestamppb.Now()
		warmItems = append(warmItems, &distqueue.ServerQueueItem{
			Item: &distqueue.QueueItem{
				Item: anyItem(&item.Item{
					Timestamp: now,
				}),
			},
			PrevTs: lastTS,
			Ts:     now,
		})
		lastTS = now
	}
	recvItems := []*distqueue.ServerQueueItem{}
	for i := 0; i < 3; i++ {
		now := timestamppb.Now()
		recvItems = append(recvItems, &distqueue.ServerQueueItem{
			Item: &distqueue.QueueItem{
				Item: anyItem(&item.Item{
					Timestamp: timestamppb.Now(),
				}),
			},
			PrevTs: lastTS,
			Ts:     now,
		})
		lastTS = now
	}
	q.warmStart.items = make(chan *distqueue.ServerQueueItem, len(warmItems))
	for _, item := range warmItems {
		q.warmStart.items <- item
	}
	close(q.warmStart.items)

	for _, item := range recvItems {
		connectClient.EXPECT().Recv().Return(item, nil)
	}
	connectClient.EXPECT().Recv().Return(nil, io.EOF)

	allItems := []*item.Item{}
	doneReceiving := make(chan bool, 1)
	go func() {
		for item := range q.queueChan {
			allItems = append(allItems, decodeAnyItem(item))
		}
		close(doneReceiving)
	}()

	err := q.receive()
	assert.Nil(t, err)
	close(q.queueChan)

	<-doneReceiving

	for i, item := range warmItems {
		assert.Equal(t, decodeAnyItem(item.Item.Item).Timestamp.AsTime().Nanosecond(), allItems[i].Timestamp.AsTime().Nanosecond())
	}
	for i, item := range recvItems {
		assert.Equal(t, decodeAnyItem(item.Item.Item).Timestamp.AsTime().Nanosecond(), allItems[i+len(warmItems)].Timestamp.AsTime().Nanosecond())
	}
}

func TestReceive_WarmStartProcessErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)

	expectedErr := fmt.Errorf("my error")
	q := &Queue{
		dq:        connectClient,
		queueChan: make(chan *anypb.Any, 1),
		ctx:       context.Background(),
		receiveHandlerChain: []ReceiveHandler{func(sqi *distqueue.ServerQueueItem, e error) (*distqueue.ServerQueueItem, error) {
			return nil, expectedErr
		}},
	}

	q.warmStart.items = make(chan *distqueue.ServerQueueItem, 1)
	q.warmStart.items <- &distqueue.ServerQueueItem{}
	close(q.warmStart.items)

	err := q.receive()
	assert.Equal(t, expectedErr, err)
}

func TestReceive_WarmStartOverlap(t *testing.T) {
	ctrl := gomock.NewController(t)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)

	q := &Queue{
		dq:        connectClient,
		queueChan: make(chan *anypb.Any, 1),
		ctx:       context.Background(),
	}

	warmItems := []*distqueue.ServerQueueItem{}
	var lastTS *timestamppb.Timestamp
	for i := 0; i < 2; i++ {
		now := timestamppb.Now()
		warmItems = append(warmItems, &distqueue.ServerQueueItem{
			Item: &distqueue.QueueItem{
				Item: anyItem(&item.Item{
					Timestamp: now,
				}),
			},
			PrevTs: lastTS,
			Ts:     now,
		})
		lastTS = now
	}
	recvItems := []*distqueue.ServerQueueItem{warmItems[len(warmItems)-1]}
	for i := 0; i < 1; i++ {
		now := timestamppb.Now()
		recvItems = append(recvItems, &distqueue.ServerQueueItem{
			Item: &distqueue.QueueItem{
				Item: anyItem(&item.Item{
					Timestamp: timestamppb.Now(),
				}),
			},
			PrevTs: lastTS,
			Ts:     now,
		})
		lastTS = now
	}
	q.warmStart.items = make(chan *distqueue.ServerQueueItem, len(warmItems))
	for _, item := range warmItems {
		q.warmStart.items <- item
	}
	close(q.warmStart.items)

	for _, item := range recvItems {
		connectClient.EXPECT().Recv().Return(item, nil)
	}
	connectClient.EXPECT().Recv().Return(nil, io.EOF)

	allItems := []*item.Item{}
	doneReceiving := make(chan bool, 1)
	go func() {
		for item := range q.queueChan {
			allItems = append(allItems, decodeAnyItem(item))
		}
		fmt.Printf("done receiving: %d\n", len(allItems))
		close(doneReceiving)
	}()

	err := q.receive()
	assert.Nil(t, err)
	close(q.queueChan)

	<-doneReceiving

	for i, item := range warmItems {
		assert.Equal(t, decodeAnyItem(item.Item.Item).Timestamp.AsTime().Nanosecond(), allItems[i].Timestamp.AsTime().Nanosecond())
	}
	for i, item := range recvItems {
		assert.Equal(t, decodeAnyItem(item.Item.Item).Timestamp.AsTime().Nanosecond(), allItems[i+len(warmItems)-1].Timestamp.AsTime().Nanosecond())
	}
}

func TestReceive_WarmStartErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)

	q := &Queue{
		dq:        connectClient,
		queueChan: make(chan *anypb.Any, 1),
		ctx:       context.Background(),
	}

	itemCount := 0
	allItemsDone := make(chan bool, 1)
	go func() {
		for range q.queueChan {
			itemCount++
		}
		allItemsDone <- true
	}()

	q.warmStart.items = make(chan *distqueue.ServerQueueItem, 1)
	receiveDone := make(chan error, 1)
	go func() {
		receiveDone <- q.receive()
	}()

	q.warmStart.items <- &distqueue.ServerQueueItem{Item: &distqueue.QueueItem{Item: &anypb.Any{}}}
	q.warmStart.errLock.Lock()
	expectedErr := fmt.Errorf("my error")
	q.warmStart.err = expectedErr
	q.warmStart.errLock.Unlock()
	q.warmStart.items <- &distqueue.ServerQueueItem{Item: &distqueue.QueueItem{Item: &anypb.Any{}}}
	close(q.warmStart.items)

	assert.Equal(t, expectedErr, <-receiveDone)
	close(q.queueChan)

	<-allItemsDone

	assert.Equal(t, 2, itemCount)

}

func TestPop_Done(t *testing.T) {
	ctrl := gomock.NewController(t)
	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)

	connectClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)
	dqClient.EXPECT().Connect(gomock.Any()).Return(connectClient, nil)

	sentItems := make(chan *distqueue.ServerQueueItem, 1)
	connectClient.EXPECT().Send(gomock.Any()).DoAndReturn(func(sqi *distqueue.QueueItem) error {
		sentItems <- &distqueue.ServerQueueItem{
			Ts:   timestamppb.Now(),
			Item: sqi,
		}
		return nil
	})
	connectClient.EXPECT().Recv().AnyTimes().DoAndReturn(func() (*distqueue.ServerQueueItem, error) {
		return <-sentItems, nil
	})
	connectClient.EXPECT().CloseSend()

	q, done, err := new(dqClient)
	assert.Nil(t, err)

	err = q.Push(anyItem(&item.Item{
		Id: "test",
	}))
	assert.Nil(t, err)

	done()

	time.Sleep(time.Second * 1) // enough time item to come in
	item, err := q.Pop()
	assert.Nil(t, err)
	assert.Equal(t, "test", decodeAnyItem(item).Id)

	time.Sleep(time.Second * 1) // enough time for finish

	item, err = q.Pop()
	assert.Nil(t, item)
	assert.Equal(t, io.EOF, err)

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

	nilItem, err := q.Pop()
	assert.Nil(t, err)
	assert.Nil(t, nilItem)

	go q.receive()

	// wait until item received
	<-itemReceived

	unmarshalledItem := item.Item{}
	poppedItem, err := q.Pop()
	assert.Nil(t, err)

	err = poppedItem.UnmarshalTo(&unmarshalledItem)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, queueItem.Id, unmarshalledItem.Id)

	nilItem, err = q.Pop()
	assert.Nil(t, err)
	assert.Nil(t, nilItem)
}

func TestDoWarmStart_Disabled(t *testing.T) {
	q := &Queue{}
	q.warmStart.enabled = false
	err := q.doWarmStart(nil)
	assert.Nil(t, err)
	assert.Nil(t, q.warmStart.items)
}

func TestDoWarmStart(t *testing.T) {
	q := &Queue{}
	q.warmStart.enabled = true
	now := time.Now()
	q.warmStart.after = &now

	ctrl := gomock.NewController(t)
	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)

	getRangeStream := mock_distqueue.NewMockDistributedQueueService_GetRangeClient(ctrl)
	dqClient.EXPECT().GetRange(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, in *distqueue.GetRangeRequest, opts ...grpc.CallOption) (distqueue.DistributedQueueService_GetRangeClient, error) {
		return getRangeStream, nil
	})

	numItems := 15
	count := 0
	getRangeStream.EXPECT().Recv().Times(numItems + 1).DoAndReturn(func() (*distqueue.ServerQueueItem, error) {
		count++
		if count > numItems {
			return nil, io.EOF
		}
		return &distqueue.ServerQueueItem{Ts: timestamppb.Now()}, nil
	})

	err := q.doWarmStart(dqClient)
	assert.Nil(t, err)
	assert.NotNil(t, q.warmStart.items)
	itemCount := 0
	for range q.warmStart.items {
		itemCount++
	}

	assert.Equal(t, numItems, itemCount)
	assert.Nil(t, q.warmStart.err)
}

func TestDoWarmStart_RecvErr(t *testing.T) {
	q := &Queue{}
	q.warmStart.enabled = true

	ctrl := gomock.NewController(t)
	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)

	getRangeStream := mock_distqueue.NewMockDistributedQueueService_GetRangeClient(ctrl)
	dqClient.EXPECT().GetRange(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, in *distqueue.GetRangeRequest, opts ...grpc.CallOption) (distqueue.DistributedQueueService_GetRangeClient, error) {
		return getRangeStream, nil
	})

	count := 0
	getRangeStream.EXPECT().Recv().Times(2).DoAndReturn(func() (*distqueue.ServerQueueItem, error) {
		if count > 0 {
			return nil, context.Canceled
		}
		count++
		return &distqueue.ServerQueueItem{Ts: timestamppb.Now()}, nil
	})

	err := q.doWarmStart(dqClient)
	assert.Nil(t, err)
	assert.NotNil(t, q.warmStart.items)

	itemCount := 0
	for range q.warmStart.items {
		itemCount++
	}
	assert.Equal(t, 1, itemCount)
	assert.Equal(t, context.Canceled, q.warmStart.err)
}

func TestDoWarmStart_Err(t *testing.T) {
	q := &Queue{}
	q.warmStart.enabled = true

	ctrl := gomock.NewController(t)
	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)

	expectedErr := fmt.Errorf("my error")
	dqClient.EXPECT().GetRange(gomock.Any(), gomock.Any()).Return(nil, expectedErr)

	err := q.doWarmStart(dqClient)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, q.warmStart.items)
}

func TestNew_DoWarmStart_Err(t *testing.T) {
	ctrl := gomock.NewController(t)
	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)

	expectedErr := fmt.Errorf("my error")

	streamClient := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)
	streamClient.EXPECT().Recv().AnyTimes().Return(nil, io.EOF)
	streamClient.EXPECT().CloseSend().AnyTimes()

	dqClient.EXPECT().Connect(gomock.Any()).Return(streamClient, nil)
	dqClient.EXPECT().GetRange(gomock.Any(), gomock.Any()).Return(nil, expectedErr)

	_, _, err := new(dqClient, WarmStart(nil))
	assert.Equal(t, expectedErr, err)
}
