package clientcmd

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/Cameron-Kurotori/microservices-coding-challenge/client"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/cmd/clientcmd/servermodels"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/item"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newItemQueue(target string, fromStart bool) (*itemQueue, error) {
	opts := []client.QueueOpt{
		client.WithReceiveHandler(client.MonitorMissing()),
	}
	if fromStart {
		opts = append(opts, client.WarmStart(nil))
	}
	queue, err := client.New(target, opts...)
	if err != nil {
		return nil, err
	}

	return &itemQueue{
		queue: queue,
	}, nil

}

type itemQueue struct {
	queue *client.Queue
}

func (server *itemQueue) push(w http.ResponseWriter, r *http.Request) *servermodels.Error {
	decoder := json.NewDecoder(r.Body)

	decoder.DisallowUnknownFields()
	var itemRequest servermodels.Item

	traceID := r.Header.Get(traceHeader)

	err := decoder.Decode(&itemRequest)
	if err != nil {
		errResp := servermodels.Error{
			StatusCode: http.StatusBadRequest,
			TraceID:    traceID,
		}
		if err == io.EOF {
			errResp.ErrorMessage = "request body is required"
		} else {
			errResp.ErrorMessage = "invalid request body: " + err.Error()
		}
		return &errResp
	}

	if itemRequest.Int == nil {
		return &servermodels.Error{
			ErrorMessage: "field 'int' must be specified",
			StatusCode:   http.StatusBadRequest,
			TraceID:      traceID,
		}
	}

	anyItem, _ := anypb.New(&item.Item{
		Id:        uuid.New().String(),
		Int:       int64(*itemRequest.Int),
		Timestamp: timestamppb.New(time.Now()),
	})

	err = server.queue.Push(anyItem)
	if err != nil {
		// TODO add logging framework
		_ = level.Error(log.NewJSONLogger(os.Stderr)).Log("msg", "error while pushing item to queue", "err", err, "trace_id", traceID)
		return &servermodels.Error{
			ErrorMessage: "internal error",
			StatusCode:   http.StatusInternalServerError,
			TraceID:      traceID,
		}
	}

	w.WriteHeader(http.StatusNoContent)
	_, _ = w.Write(nil)
	return nil
}

func pushHandler(itemServer *itemQueue) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		err := itemServer.push(rw, r)
		if err != nil {
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(err.StatusCode)
			_, _ = rw.Write(err.Marshal())
		}
	})
}

func (server *itemQueue) pop(w http.ResponseWriter, r *http.Request) *servermodels.Error {

	traceID := r.Header.Get(traceHeader)

	anyItem := server.queue.Pop()
	if anyItem == nil {
		w.WriteHeader(http.StatusNoContent)
		_, _ = w.Write(nil)
		return nil
	}

	var item item.Item
	err := anyItem.UnmarshalTo(&item)
	if err != nil {
		// TODO add logging framework
		_ = level.Error(log.NewJSONLogger(os.Stderr)).Log("msg", "error while decoding popped item", "err", err, "trace_id", traceID)
		return &servermodels.Error{
			ErrorMessage: "internal error",
			StatusCode:   http.StatusInternalServerError,
			TraceID:      traceID,
		}
	}

	resp := servermodels.Item{
		Int: &item.Int,
	}

	respBody, err := json.Marshal(resp)
	if err != nil {
		// TODO add logging framework
		_ = level.Error(log.NewJSONLogger(os.Stderr)).Log("msg", "error while marshalling response body", "err", err, "trace_id", traceID)
		return &servermodels.Error{
			ErrorMessage: "internal error",
			StatusCode:   http.StatusInternalServerError,
			TraceID:      traceID,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBody)

	return nil
}

func popHandler(itemServer *itemQueue) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		err := itemServer.pop(rw, r)
		if err != nil {
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(err.StatusCode)
			_, _ = rw.Write(err.Marshal())
		}
	})
}
