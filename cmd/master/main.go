package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/darshanmadesh/godfs/api"
	"github.com/darshanmadesh/godfs/internal/master"
)

func main() {
	// Parse command-line flags.
	// flag package provides simple CLI argument parsing.
	// Format: flag.Type(name, default, description)
	port := flag.Int("port", 50051, "Port to listen on")
	dataDir := flag.String("data-dir", "./data", "Directory to store file data")
	flag.Parse() // Actually parse os.Args

	// Create the DFS server
	dfsServer, err := master.NewServer(*dataDir)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Create a TCP listener on the specified port.
	// net.Listen returns a Listener interface that accepts connections.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", *port, err)
	}

	// Create the gRPC server.
	// grpc.NewServer() returns a Server that can register services
	// and serve requests.
	grpcServer := grpc.NewServer()

	// Register our DFS service with the gRPC server.
	// This tells gRPC to route FileService RPCs to our dfsServer.
	api.RegisterFileServiceServer(grpcServer, dfsServer)

	// Set up graceful shutdown.
	// This is a production best practice - handle SIGINT (Ctrl+C) and SIGTERM
	// gracefully to finish in-flight requests before shutting down.
	//
	// How it works:
	// 1. signal.Notify sends OS signals to the 'stop' channel
	// 2. A goroutine waits on the channel
	// 3. When signal received, it calls GracefulStop()

	stop := make(chan os.Signal, 1)
	// Notify this channel on SIGINT (Ctrl+C) or SIGTERM (docker stop, kill)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine to handle shutdown.
	// Goroutines are lightweight threads managed by Go runtime.
	// The 'go' keyword starts a function in a new goroutine.
	go func() {
		<-stop // Block until signal received (channel receive)
		log.Println("Shutting down server...")

		// GracefulStop stops accepting new connections and waits
		// for existing RPCs to complete before stopping.
		grpcServer.GracefulStop()
	}()

	// Log server startup information
	log.Printf("GoDFS Master Server starting...")
	log.Printf("  Port:     %d", *port)
	log.Printf("  Data dir: %s", *dataDir)
	log.Println("Press Ctrl+C to stop")

	// Start serving requests.
	// Serve() blocks until the server stops.
	// This is why we handle shutdown in a separate goroutine.
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

	log.Println("Server stopped")
}
