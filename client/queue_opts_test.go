package client

import (
	"io"
	"sync/atomic"
	"testing"

	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue/mock_distqueue"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/item"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWithReceiveHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	dqClient := mock_distqueue.NewMockDistributedQueueServiceClient(ctrl)
	stream := mock_distqueue.NewMockDistributedQueueService_ConnectClient(ctrl)
	dqClient.EXPECT().Connect(gomock.Any()).Return(stream, nil)
	var count int32

	badItem := &item.Item{
		Id: "bad",
	}
	goodItem := &item.Item{
		Id: "good",
	}
	stream.EXPECT().CloseSend()
	stream.EXPECT().Recv().Times(7).DoAndReturn(func() (*distqueue.ServerQueueItem, error) {
		if count > 5 {
			return nil, io.EOF
		}
		if count > 4 {
			return &distqueue.ServerQueueItem{
				Item: &distqueue.QueueItem{
					Item: anyItem(goodItem),
				},
			}, nil
		}
		return &distqueue.ServerQueueItem{
			Item: &distqueue.QueueItem{
				Item: anyItem(badItem),
			},
		}, nil
	})
	queue, err := new(dqClient, WithReceiveHandler(func(sqi *distqueue.ServerQueueItem, e error) (*distqueue.ServerQueueItem, error) {
		atomic.AddInt32(&count, 1)
		return sqi, e
	}, func(sqi *distqueue.ServerQueueItem, e error) (*distqueue.ServerQueueItem, error) {
		if e == nil && decodeAnyItem(sqi.Item.Item).Id == badItem.Id {
			return nil, nil
		}
		return sqi, e
	}))
	if err != nil {
		t.Fatal(err)
	}

	<-queue.ReceiveErrors
	queue.Done()

	item, ok := <-queue.queueChan
	assert.True(t, ok)
	assert.Equal(t, goodItem.Id, decodeAnyItem(item).Id)

	item, ok = <-queue.queueChan
	assert.False(t, ok)
	assert.Nil(t, item)
}

func TestMonitorMissing(t *testing.T) {
	monitor := MonitorMissing()
	now := timestamppb.Now()
	_, err := monitor(&distqueue.ServerQueueItem{
		Ts: now,
	}, nil)
	assert.Nil(t, err)
	_, err = monitor(&distqueue.ServerQueueItem{
		PrevTs: timestamppb.New(now.AsTime().Add(1)),
		Ts:     timestamppb.New(now.AsTime().Add(2)),
	}, nil)

	if err == nil {
		t.Fatal("expected error got none")
	}

	t.Logf("%+v", err)
	missingErr, ok := err.(MissingItemErr)
	if !ok {
		t.Fatal("expected missing item err")
	}

	assert.Equal(t, now.AsTime(), missingErr.expectedLastTS.AsTime())
	assert.Equal(t, now.AsTime().Add(1), missingErr.actualLastTS.AsTime())
}

func anyItem(item *item.Item) *anypb.Any {
	anyItem, _ := anypb.New(item)
	return anyItem
}

func decodeAnyItem(anyItem *anypb.Any) *item.Item {
	decoded := &item.Item{}
	_ = anyItem.UnmarshalTo(decoded)
	return decoded
}
