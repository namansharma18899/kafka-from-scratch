package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	port := "9092"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	addr := "0.0.0.0:" + port
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to bind to %s: %v\n", addr, err)
		fmt.Println("(If port 9092 is in use, try: PORT=9093 ./your_program.sh)")
		os.Exit(1)
	}
	fmt.Printf("Kafka broker listening on %s (Ctrl+C to stop)\n", addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Printf("\nReceived %v, shutting down gracefully...\n", sig)
		cancel()
		l.Close()
	}()
	RunWithListener(ctx, l)
}

// RunWithListener runs the broker accept loop on the given listener.
// When ctx is cancelled or the listener is closed, it stops accepting and drains connections.
// Used by main (with signal handler) and by tests (ctx cancel stops the server).
func RunWithListener(ctx context.Context, l net.Listener) {
	var connWg sync.WaitGroup
	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		connWg.Add(1)
		go func(c net.Conn) {
			defer connWg.Done()
			handleConnection(c)
		}(conn)
	}

	done := make(chan struct{})
	go func() {
		connWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		if ctx.Err() == nil {
			fmt.Println("All connections closed.")
		}
	case <-time.After(5 * time.Second):
		fmt.Println("Shutdown timeout; exiting.")
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		apiKey, _, correlationId, body, err := readRequestHeader(conn)
		if err != nil {
			return
		}
		var response []byte
		switch apiKey {
		case ApiKeyApiVersions:
			response = handleApiVersions(body, correlationId)
		case ApiKeyMetadata:
			response = handleMetadata(body, correlationId)
		case ApiKeyProduce:
			response = handleProduce(body, correlationId)
		case ApiKeyFetch:
			response = handleFetch(body, correlationId)
		default:
			// Unknown API key - send empty response with correlation id so client doesn't hang
			response = buildResponse(correlationId, []byte{})
		}
		if len(response) > 0 {
			if err := writeResponse(conn, correlationId, response); err != nil {
				return
			}
		}
	}
}
