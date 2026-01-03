package master

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/darshanmadesh/godfs/api"
)

// Default chunk size for streaming file transfers (1MB).
// Smaller than the 64MB chunks we'll use for storage in Phase 2,
// this is just for gRPC streaming efficiency.
const defaultChunkSize = 1024 * 1024 // 1MB

// Server implements the gRPC FileService interface.
// It coordinates metadata storage and file data storage.
type Server struct {
	// Embed the unimplemented server for forward compatibility.
	// This is a gRPC best practice - if new methods are added to the
	// proto, your code won't break (it just returns "unimplemented").
	api.UnimplementedFileServiceServer

	// metadata stores file metadata (names, sizes, timestamps)
	metadata MetadataStore

	// dataDir is where actual file data is stored on disk.
	// In Phase 1, we store complete files. In later phases,
	// this becomes chunk storage.
	dataDir string
}

// NewServer creates a new DFS master server.
// dataDir is the directory where file data will be stored.
func NewServer(dataDir string) (*Server, error) {
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	return &Server{
		metadata: NewInMemoryMetadataStore(),
		dataDir:  dataDir,
	}, nil
}

// Upload handles streaming file uploads from clients.
// The client sends: 1) metadata message, then 2) multiple chunk messages.
func (s *Server) Upload(stream api.FileService_UploadServer) error {
	var (
		filename string
		fileSize int64
		file     *os.File
	)

	// Receive messages from the stream until EOF or error
	for {
		// Recv() blocks until a message arrives or stream closes.
		// This is the core of gRPC streaming - processing one chunk at a time.
		req, err := stream.Recv()
		if err == io.EOF {
			// Client finished sending - this is the normal completion path
			break
		}
		if err != nil {
			// Clean up partial file on error
			if file != nil {
				file.Close()
				os.Remove(filepath.Join(s.dataDir, filename))
			}
			return fmt.Errorf("failed to receive chunk: %w", err)
		}

		// Handle the two types of messages using a type switch on the oneof field
		switch data := req.Data.(type) {
		case *api.UploadRequest_Metadata:
			// First message contains file metadata
			filename = data.Metadata.Filename
			fileSize = data.Metadata.Size

			// Validate filename to prevent path traversal attacks
			// This is a security best practice!
			if filepath.Base(filename) != filename {
				return errors.New("invalid filename: must not contain path separators")
			}

			// Create the file for writing
			filePath := filepath.Join(s.dataDir, filename)
			file, err = os.Create(filePath)
			if err != nil {
				return fmt.Errorf("failed to create file: %w", err)
			}

		case *api.UploadRequest_Chunk:
			// Subsequent messages contain file data chunks
			if file == nil {
				return errors.New("received chunk before metadata")
			}

			// Write chunk to file
			if _, err := file.Write(data.Chunk); err != nil {
				file.Close()
				os.Remove(filepath.Join(s.dataDir, filename))
				return fmt.Errorf("failed to write chunk: %w", err)
			}
		}
	}

	// Close the file
	if file != nil {
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}
	}

	// Store metadata
	meta := &FileMeta{
		Filename: filename,
		Size:     fileSize,
	}
	if err := s.metadata.Create(meta); err != nil {
		// If metadata creation fails, remove the data file
		os.Remove(filepath.Join(s.dataDir, filename))
		return fmt.Errorf("failed to store metadata: %w", err)
	}

	// Send success response
	return stream.SendAndClose(&api.UploadResponse{
		Success: true,
		Message: fmt.Sprintf("File '%s' uploaded successfully", filename),
		FileId:  filename, // In Phase 1, filename is the ID
	})
}

// Download handles streaming file downloads to clients.
// The server sends: 1) metadata message, then 2) multiple chunk messages.
func (s *Server) Download(req *api.DownloadRequest, stream api.FileService_DownloadServer) error {
	filename := req.Filename

	// Get file metadata
	meta, err := s.metadata.Get(filename)
	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			return fmt.Errorf("file not found: %s", filename)
		}
		return fmt.Errorf("failed to get metadata: %w", err)
	}

	// Open the file for reading
	filePath := filepath.Join(s.dataDir, filename)
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close() // Always close file when function returns

	// Send metadata as first message
	if err := stream.Send(&api.DownloadResponse{
		Data: &api.DownloadResponse_Metadata{
			Metadata: &api.FileMetadata{
				Filename: meta.Filename,
				Size:     meta.Size,
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send metadata: %w", err)
	}

	// Stream file data in chunks
	// Using a buffer avoids allocating memory for each chunk
	buf := make([]byte, defaultChunkSize)
	for {
		// Read up to defaultChunkSize bytes
		n, err := file.Read(buf)
		if err == io.EOF {
			// Finished reading file
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		// Send the chunk (only the bytes we read, not the full buffer)
		if err := stream.Send(&api.DownloadResponse{
			Data: &api.DownloadResponse_Chunk{
				Chunk: buf[:n],
			},
		}); err != nil {
			return fmt.Errorf("failed to send chunk: %w", err)
		}
	}

	return nil
}

// List returns metadata for all files matching the prefix filter.
func (s *Server) List(ctx context.Context, req *api.ListRequest) (*api.ListResponse, error) {
	// The context carries cancellation signals and deadlines.
	// We should check ctx.Done() for long operations, but List is fast.

	files, err := s.metadata.List(req.Prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	// Convert internal FileMeta to API FileInfo
	fileInfos := make([]*api.FileInfo, 0, len(files))
	for _, f := range files {
		fileInfos = append(fileInfos, &api.FileInfo{
			Filename:   f.Filename,
			Size:       f.Size,
			CreatedAt:  f.CreatedAt.Unix(),
			ModifiedAt: f.ModifiedAt.Unix(),
		})
	}

	return &api.ListResponse{
		Files: fileInfos,
	}, nil
}

// Delete removes a file from the DFS.
func (s *Server) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	filename := req.Filename

	// Check if file exists
	if !s.metadata.Exists(filename) {
		return &api.DeleteResponse{
			Success: false,
			Message: fmt.Sprintf("file not found: %s", filename),
		}, nil
	}

	// Delete the actual file data first
	filePath := filepath.Join(s.dataDir, filename)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to delete file data: %w", err)
	}

	// Delete metadata
	if err := s.metadata.Delete(filename); err != nil {
		return nil, fmt.Errorf("failed to delete metadata: %w", err)
	}

	return &api.DeleteResponse{
		Success: true,
		Message: fmt.Sprintf("File '%s' deleted successfully", filename),
	}, nil
}

// Stat returns metadata for a specific file.
func (s *Server) Stat(ctx context.Context, req *api.StatRequest) (*api.StatResponse, error) {
	meta, err := s.metadata.Get(req.Filename)
	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			return &api.StatResponse{
				Exists: false,
			}, nil
		}
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	return &api.StatResponse{
		Exists: true,
		File: &api.FileInfo{
			Filename:   meta.Filename,
			Size:       meta.Size,
			CreatedAt:  meta.CreatedAt.Unix(),
			ModifiedAt: meta.ModifiedAt.Unix(),
		},
	}, nil
}
