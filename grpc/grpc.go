package grpc

import (
	"context"
	"os"
	"time"

	"github.com/go-kit/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewServer() *grpc.Server {
	opts := []grpc.ServerOption{}
	logger := log.NewJSONLogger(os.Stderr)
	opts = append(opts, grpc.StreamInterceptor(logStreamServerInterceptor(logger)))
	server := grpc.NewServer(opts...)
	return server
}

func NewClient(target string) (grpc.ClientConnInterface, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	dialOptions := []grpc.DialOption{}

	logger := log.NewJSONLogger(os.Stderr)
	logger = log.With(logger, "grpc_target", target)
	dialOptions = append(dialOptions,
		grpc.WithStreamInterceptor(logStreamClientInterceptor(logger)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	return grpc.DialContext(ctx, target, dialOptions...)
}

func logStreamClientInterceptor(logger log.Logger) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		logger := log.With(logger, "method", method, "stream_name", desc.StreamName)
		_ = logger.Log("msg", "outgoing grpc client stream request")
		defer func(start time.Time) {
			_ = logger.Log("msg", "outgoing grpc client stream request complete", "took_ms", time.Since(start).Milliseconds())
		}(time.Now())
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func logStreamServerInterceptor(logger log.Logger) func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		logger := log.With(logger, "method", info.FullMethod)
		_ = logger.Log("msg", "outgoing grpc server stream request")
		defer func(start time.Time) {
			_ = logger.Log("msg", "outgoing grpc server stream request complete", "took_ms", time.Since(start).Milliseconds())
		}(time.Now())
		return handler(srv, ss)
	}
}
