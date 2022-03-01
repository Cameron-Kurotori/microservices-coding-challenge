package clientcmd

import (
	"net/http"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
)

const traceHeader = "X-Trace-Id"

type loggingResponseWriter struct {
	parent        http.ResponseWriter
	contentLength int
	statusCode    int
	written       int
}

func (w *loggingResponseWriter) Header() http.Header {
	return w.parent.Header()
}

func (w *loggingResponseWriter) Write(b []byte) (int, error) {
	w.written++
	w.contentLength += len(b)
	return w.parent.Write(b)
}

func (w *loggingResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.parent.WriteHeader(statusCode)
}

func loggingMiddleware(h http.Handler) http.Handler {
	logger := log.NewJSONLogger(os.Stderr)
	logger = log.With(logger, "caller", log.DefaultCaller)
	logger = level.Debug(logger)
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		traceID := uuid.New().String()
		r.Header.Set(traceHeader, traceID) // pass it along
		logger = log.With(logger, "trace_id", traceID)
		_ = logger.Log(
			"msg", "request entry",
			"request_uri", r.RequestURI,
			"remote_addr", r.RemoteAddr,
			"request_method", r.Method,
		)
		logRW := &loggingResponseWriter{
			parent: rw,
		}
		defer func(start time.Time) {
			_ = logger.Log(
				"msg", "request finished",
				"response_statuscode", logRW.statusCode,
				"content_length", logRW.contentLength,
				"count_written", logRW.written,
				"took_ms", time.Since(start).Milliseconds(),
			)
		}(time.Now())
		h.ServeHTTP(logRW, r)
	})
}
