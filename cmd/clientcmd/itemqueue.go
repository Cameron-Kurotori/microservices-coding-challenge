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

func newItemQueue(target string, fromStart bool) (*itemQueue, func(), error) {
	opts := []client.QueueOpt{
		client.WithReceiveHandler(client.MonitorMissing()),
	}
	if fromStart {
		opts = append(opts, client.WarmStart(nil))
	}
	queue, done, err := client.New(target, opts...)
	if err != nil {
		return nil, nil, err
	}

	return &itemQueue{
		queue: queue,
	}, done, nil

}

type itemQueue struct {
	queue *client.Queue
}

// writeError is a helper function for writing an error response
func writeError(rw http.ResponseWriter, err servermodels.Error) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(err.StatusCode)
	_, _ = rw.Write(err.Marshal())
}

// internalError is a helper function to create a new internal error with trace ID
func internalError(traceID string) servermodels.Error {
	return servermodels.Error{
		ErrorMessage: "internal error",
		StatusCode:   http.StatusInternalServerError,
		TraceID:      traceID,
	}
}

// anyItem converts a servermodels.Item to Any for use with queue
func anyItem(itemRequest servermodels.Item) *anypb.Any {
	anyItem, _ := anypb.New(&item.Item{
		Id:        uuid.New().String(),
		Int:       int64(*itemRequest.Int),
		Timestamp: timestamppb.New(time.Now()),
	})
	return anyItem
}

// decodeItem converts an Any to the servermodels.Item for use with queue
func decodeItem(anyItem *anypb.Any) (servermodels.Item, error) {
	var item item.Item
	err := anyItem.UnmarshalTo(&item)
	if err != nil {
		return servermodels.Item{}, err
	}
	return servermodels.Item{
		Int: &item.Int,
	}, nil
}

// validatePushItem performs http layer syntax and sematic checks on input
func validatePushItem(itemRequest servermodels.Item) *servermodels.Error {
	if itemRequest.Int == nil {
		return &servermodels.Error{
			ErrorMessage: "field 'int' must be specified",
			StatusCode:   http.StatusBadRequest,
		}
	}
	return nil
}

// decodeItemRequest decodes the reader into a servermodels.item and performs validation
func decodeItemRequest(body io.Reader) (servermodels.Item, *servermodels.Error) {
	decoder := json.NewDecoder(body)

	decoder.DisallowUnknownFields()
	var itemRequest servermodels.Item

	err := decoder.Decode(&itemRequest)
	if err != nil {
		errResp := servermodels.Error{
			StatusCode: http.StatusBadRequest,
		}
		if err == io.EOF {
			errResp.ErrorMessage = "request body is required"
		} else {
			errResp.ErrorMessage = "invalid request body: " + err.Error()
		}

		return itemRequest, &errResp
	}
	return itemRequest, validatePushItem(itemRequest)
}

func (server *itemQueue) pushHandler(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(traceHeader)

	itemRequest, serverErr := decodeItemRequest(r.Body)
	if serverErr != nil {
		serverErr.TraceID = traceID
		writeError(w, *serverErr)
		return
	}

	err := server.queue.Push(anyItem(itemRequest))
	if err != nil {
		// TODO add logging framework
		_ = level.Error(log.NewJSONLogger(os.Stderr)).Log("msg", "error while pushing item to queue", "err", err, "trace_id", traceID)
		writeError(w, servermodels.Error{
			ErrorMessage: "internal error",
			StatusCode:   http.StatusInternalServerError,
			TraceID:      traceID,
		})
		return
	}

	w.WriteHeader(http.StatusNoContent)
	_, _ = w.Write(nil)
}

// writeItem writes the servermodels.Item response
func writeItem(w http.ResponseWriter, item servermodels.Item, traceID string) {
	body, err := json.Marshal(item)
	if err != nil {
		_ = level.Error(log.NewJSONLogger(os.Stderr)).Log("msg", "error while marshalling response body", "err", err, "trace_id", traceID)
		writeError(w, internalError(traceID))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (server *itemQueue) popHandler(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(traceHeader)

	anyItem, err := server.queue.Pop()
	if err != nil && err != io.EOF {
		// this should not happen
		_ = level.Error(log.NewJSONLogger(os.Stderr)).Log("msg", "error while popping item", "err", err, "trace_id", traceID)
		writeError(w, internalError(traceID))
		return
	}

	if anyItem == nil {
		w.WriteHeader(http.StatusNoContent)
		_, _ = w.Write(nil)
		return
	}

	item, err := decodeItem(anyItem)
	if err != nil {
		_ = level.Error(log.NewJSONLogger(os.Stderr)).Log("msg", "error while decoding queue item", "err", err, "trace_id", traceID)
		writeError(w, internalError(traceID))
		return
	}

	writeItem(w, item, traceID)
}
