package servercmd

import (
	"fmt"
	"net"
	"os"
	"os/signal"

	"github.com/Cameron-Kurotori/microservices-coding-challenge/grpc"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/proto/distqueue"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/server"
	"github.com/spf13/cobra"
)

var ServerCmd = &cobra.Command{
	Use:  "server [--host host] [--port port]",
	Long: "The server command starts up a distributed queue server",
	RunE: runServer,
}

var (
	host string
	port string
)

func init() {
	ServerCmd.Flags().StringVarP(&host, "host", "H", "0.0.0.0", "host to start server on")
	ServerCmd.Flags().StringVarP(&port, "port", "p", "6789", "port to start server on")
}

func runServer(cmd *cobra.Command, args []string) error {
	grpcServer := grpc.NewServer()
	dqServer := server.New()
	distqueue.RegisterDistributedQueueServiceServer(grpcServer, dqServer)

	ln, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		return err
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	var errChan = make(chan error, 1)
	go func() {
		fmt.Fprintf(os.Stderr, "signal received: %v\n", <-signalChan)
		errChan <- nil
	}()

	go func() {
		fmt.Fprintf(os.Stderr, "server serving on %s ...\n", host+":"+port)
		errChan <- grpcServer.Serve(ln)
	}()
	defer func() {
		fmt.Fprintln(os.Stderr, "gracefully stopping server...")
		grpcServer.GracefulStop()
		fmt.Fprintln(os.Stderr, "stopped")
	}()

	return <-errChan
}
