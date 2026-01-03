package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/darshanmadesh/godfs/api"
)

// Chunk size for streaming uploads (1MB)
const chunkSize = 1024 * 1024

func main() {
	// Define flags that apply to all commands
	serverAddr := flag.String("server", "localhost:50051", "Server address (host:port)")

	// Custom usage message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "GoDFS Client - A distributed file system client\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s [flags] <command> [arguments]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  upload <local-file>              Upload a file to DFS\n")
		fmt.Fprintf(os.Stderr, "  download <remote-file> [local]   Download a file from DFS\n")
		fmt.Fprintf(os.Stderr, "  list [prefix]                    List files in DFS\n")
		fmt.Fprintf(os.Stderr, "  delete <filename>                Delete a file from DFS\n")
		fmt.Fprintf(os.Stderr, "  stat <filename>                  Get file information\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	// Get the command (first non-flag argument)
	args := flag.Args()
	if len(args) < 1 {
		flag.Usage()
		os.Exit(1)
	}
	command := args[0]
	cmdArgs := args[1:]

	// Create gRPC connection to the server.
	// grpc.Dial establishes a connection to the server.
	// WithTransportCredentials(insecure.NewCredentials()) disables TLS.
	// In production, you'd use proper TLS credentials!
	conn, err := grpc.NewClient(
		*serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close() // Always close connection when done

	// Create the FileService client.
	// This is generated from our proto file - it provides typed methods
	// for all our RPCs.
	client := api.NewFileServiceClient(conn)

	// Create a context with timeout for all operations.
	// Context carries deadlines and cancellation signals across API boundaries.
	// 5 minutes is generous for large file transfers.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel() // Release resources associated with context

	// Route to the appropriate command handler
	var cmdErr error
	switch command {
	case "upload":
		cmdErr = handleUpload(ctx, client, cmdArgs)
	case "download":
		cmdErr = handleDownload(ctx, client, cmdArgs)
	case "list":
		cmdErr = handleList(ctx, client, cmdArgs)
	case "delete":
		cmdErr = handleDelete(ctx, client, cmdArgs)
	case "stat":
		cmdErr = handleStat(ctx, client, cmdArgs)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		flag.Usage()
		os.Exit(1)
	}

	if cmdErr != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", cmdErr)
		os.Exit(1)
	}
}

// handleUpload uploads a local file to the DFS.
func handleUpload(ctx context.Context, client api.FileServiceClient, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: upload <local-file>")
	}

	localPath := args[0]

	// Open the local file
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info for metadata
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// Start the upload stream.
	// Upload returns a stream we can send messages on.
	stream, err := client.Upload(ctx)
	if err != nil {
		return fmt.Errorf("failed to start upload: %w", err)
	}

	// Send metadata as first message
	if err := stream.Send(&api.UploadRequest{
		Data: &api.UploadRequest_Metadata{
			Metadata: &api.FileMetadata{
				Filename: filepath.Base(localPath), // Use just the filename, not full path
				Size:     fileInfo.Size(),
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send metadata: %w", err)
	}

	// Stream file data in chunks
	buf := make([]byte, chunkSize)
	var totalSent int64

	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		// Send chunk
		if err := stream.Send(&api.UploadRequest{
			Data: &api.UploadRequest_Chunk{
				Chunk: buf[:n],
			},
		}); err != nil {
			return fmt.Errorf("failed to send chunk: %w", err)
		}

		totalSent += int64(n)

		// Print progress (simple progress indicator)
		progress := float64(totalSent) / float64(fileInfo.Size()) * 100
		fmt.Printf("\rUploading... %.1f%%", progress)
	}

	// Close the stream and get the response.
	// CloseAndRecv() signals we're done sending and waits for server response.
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	fmt.Printf("\r") // Clear progress line
	if resp.Success {
		fmt.Printf("Uploaded '%s' successfully (%d bytes)\n", filepath.Base(localPath), totalSent)
	} else {
		return fmt.Errorf("server error: %s", resp.Message)
	}

	return nil
}

// handleDownload downloads a file from the DFS to local filesystem.
func handleDownload(ctx context.Context, client api.FileServiceClient, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: download <remote-file> [local-path]")
	}

	remoteFile := args[0]
	localPath := remoteFile // Default: same name as remote
	if len(args) > 1 {
		localPath = args[1]
	}

	// Start the download stream
	stream, err := client.Download(ctx, &api.DownloadRequest{
		Filename: remoteFile,
	})
	if err != nil {
		return fmt.Errorf("failed to start download: %w", err)
	}

	// Receive the first message (should be metadata)
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive metadata: %w", err)
	}

	metadata, ok := resp.Data.(*api.DownloadResponse_Metadata)
	if !ok {
		return fmt.Errorf("expected metadata, got chunk")
	}

	fileSize := metadata.Metadata.Size

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Receive and write chunks
	var totalReceived int64
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Clean up partial file on error
			os.Remove(localPath)
			return fmt.Errorf("download failed: %w", err)
		}

		chunk, ok := resp.Data.(*api.DownloadResponse_Chunk)
		if !ok {
			continue // Skip non-chunk messages
		}

		n, err := file.Write(chunk.Chunk)
		if err != nil {
			os.Remove(localPath)
			return fmt.Errorf("failed to write to file: %w", err)
		}

		totalReceived += int64(n)

		// Print progress
		progress := float64(totalReceived) / float64(fileSize) * 100
		fmt.Printf("\rDownloading... %.1f%%", progress)
	}

	fmt.Printf("\r") // Clear progress line
	fmt.Printf("Downloaded '%s' to '%s' (%d bytes)\n", remoteFile, localPath, totalReceived)

	return nil
}

// handleList lists files in the DFS.
func handleList(ctx context.Context, client api.FileServiceClient, args []string) error {
	prefix := ""
	if len(args) > 0 {
		prefix = args[0]
	}

	resp, err := client.List(ctx, &api.ListRequest{
		Prefix: prefix,
	})
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	if len(resp.Files) == 0 {
		fmt.Println("No files found")
		return nil
	}

	// Print header
	fmt.Printf("%-40s %15s %20s\n", "FILENAME", "SIZE", "MODIFIED")
	fmt.Println(repeat("-", 77))

	// Print each file
	for _, f := range resp.Files {
		modTime := time.Unix(f.ModifiedAt, 0).Format("2006-01-02 15:04:05")
		fmt.Printf("%-40s %15s %20s\n", f.Filename, formatSize(f.Size), modTime)
	}

	fmt.Printf("\nTotal: %d file(s)\n", len(resp.Files))

	return nil
}

// handleDelete deletes a file from the DFS.
func handleDelete(ctx context.Context, client api.FileServiceClient, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: delete <filename>")
	}

	filename := args[0]

	resp, err := client.Delete(ctx, &api.DeleteRequest{
		Filename: filename,
	})
	if err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}

	if resp.Success {
		fmt.Printf("Deleted '%s'\n", filename)
	} else {
		fmt.Printf("Failed to delete: %s\n", resp.Message)
	}

	return nil
}

// handleStat gets information about a file.
func handleStat(ctx context.Context, client api.FileServiceClient, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: stat <filename>")
	}

	filename := args[0]

	resp, err := client.Stat(ctx, &api.StatRequest{
		Filename: filename,
	})
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	if !resp.Exists {
		fmt.Printf("File '%s' does not exist\n", filename)
		return nil
	}

	f := resp.File
	created := time.Unix(f.CreatedAt, 0).Format("2006-01-02 15:04:05")
	modified := time.Unix(f.ModifiedAt, 0).Format("2006-01-02 15:04:05")

	fmt.Printf("Filename: %s\n", f.Filename)
	fmt.Printf("Size:     %s (%d bytes)\n", formatSize(f.Size), f.Size)
	fmt.Printf("Created:  %s\n", created)
	fmt.Printf("Modified: %s\n", modified)

	return nil
}

// formatSize formats bytes into human-readable format.
func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// repeat returns a string with char repeated n times.
func repeat(char string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += char
	}
	return result
}
