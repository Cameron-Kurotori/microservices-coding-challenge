package clientcmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
)

var ClientCmd = &cobra.Command{
	Use:  "client --target target [--host host] [--port port]",
	RunE: runClient,
}

var (
	target    string
	host      string
	port      string
	fromStart bool
)

func init() {
	ClientCmd.Flags().StringVarP(&target, "target", "t", "", "server target to connect to")
	ClientCmd.Flags().StringVarP(&host, "host", "H", "0.0.0.0", "host to start client server on")
	ClientCmd.Flags().StringVarP(&port, "port", "p", "2345", "port to start client server on")
	ClientCmd.Flags().BoolVar(&fromStart, "from-start", false, "get all items from the start of the server")
	err := ClientCmd.MarkFlagRequired("target")
	if err != nil {
		panic(err)
	}
}

func runClient(cmd *cobra.Command, args []string) error {
	var errChan = make(chan error, 1)
	router := mux.NewRouter()

	itemQueue, err := newItemQueue(target, fromStart)
	if err != nil {
		return err
	}

	go func() {
		errChan <- (<-itemQueue.queue.ReceiveErrors)
	}()

	defer func() {
		fmt.Fprintf(os.Stderr, "item queue client stopping...\n")
		itemQueue.queue.Done()
		fmt.Fprintf(os.Stderr, "stopped\n")
	}()

	setupRouter(router, itemQueue)
	root := setupMiddleware(router)

	server := http.Server{
		Addr:    host + ":" + port,
		Handler: root,
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		fmt.Fprintf(os.Stderr, "signal received: %v\n", <-signalChan)
		errChan <- nil
	}()

	go func() {
		fmt.Fprintf(os.Stderr, "client serving on %s ...\n", "http://"+host+":"+port)
		errChan <- server.ListenAndServe()
	}()

	defer func() {
		fmt.Fprintln(os.Stderr, "gracefully stopping server...")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		err := server.Shutdown(ctx)
		fmt.Fprintf(os.Stderr, "stopped: %v\n", err)
	}()

	return <-errChan
}

func setupRouter(router *mux.Router, itemQueue *itemQueue) {
	router.Path("/item").Methods(http.MethodPost).Handler(pushHandler(itemQueue))
	router.Path("/item").Methods(http.MethodGet).Handler(popHandler(itemQueue))
}

func setupMiddleware(handler http.Handler) http.Handler {
	middleware := []func(http.Handler) http.Handler{
		loggingMiddleware,
	}
	for _, m := range middleware {
		handler = m(handler)
	}
	return handler
}
