package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Frame struct {
	Data []byte
}

type FrameReader interface {
	ReadFrame() (Frame, error)
	ReadFrames() ([]Frame, error)
}

type FrameReaderError struct {
	Message string
}

func (e FrameReaderError) Error() string {
	return e.Message
}

type FrameWriter interface {
	WriteFrame(frame Frame) error
	WriteFrames(frames []Frame) error
}

type FrameWriterError struct {
	Message string
}

func (e FrameWriterError) Error() string {
	return e.Message
}

type ConnFrameReader struct {
	conn         net.Conn
	reader       *bufio.Reader
	maxFrameSize uint32
}

func NewConnFrameReader(conn net.Conn, bufferSize int) *ConnFrameReader {
	return &ConnFrameReader{
		conn:         conn,
		reader:       bufio.NewReaderSize(conn, bufferSize),
		maxFrameSize: 1024 * 1024,
	}
}

func (cfr *ConnFrameReader) ReadFrame() (Frame, error) {
	// Read the frame length (4 bytes, big endian)
	lengthBytes := make([]byte, 4)
	_, err := io.ReadFull(cfr.reader, lengthBytes)
	if err != nil {
		return Frame{}, FrameWriterError{Message: fmt.Sprintf("error reading frame length bytes: %v", err)}
	}

	frameLength := binary.BigEndian.Uint32(lengthBytes)

	// Sanity check for frame length (prevent excessive memory allocation)
	if frameLength > cfr.maxFrameSize {
		return Frame{}, FrameWriterError{Message: "frame too large"}
	}

	payload := make([]byte, frameLength)
	_, err = io.ReadFull(cfr.reader, payload)
	if err != nil {
		return Frame{}, FrameWriterError{Message: fmt.Sprintf("error reading frame payload bytes: %v", err)}
	}

	return Frame{Data: payload}, nil
}

// ReadFrames reads one frame (blocking), then reads all available frames without blocking
func (cfr *ConnFrameReader) ReadFrames() ([]Frame, error) {
	frames := make([]Frame, 0)

	// First, read at least one frame (blocking)
	frame, err := cfr.ReadFrame()
	if err != nil {
		return nil, err
	}
	frames = append(frames, frame)

	// Now read additional frames that are immediately available (non-blocking)
	for {
		// Check if we have enough buffered data for at least the frame header
		if cfr.reader.Buffered() < 4 {
			break
		}

		// Peek at the length to see if we have a complete frame
		lengthBytes, err := cfr.reader.Peek(4)
		if err != nil || len(lengthBytes) < 4 {
			break
		}

		frameLength := binary.BigEndian.Uint32(lengthBytes)
		totalFrameSize := 4 + int(frameLength)

		// Check if the complete frame is available in the buffer
		if cfr.reader.Buffered() < totalFrameSize {
			break
		}

		// Read the next frame
		nextFrame, err := cfr.ReadFrame()
		if err != nil {
			break
		}
		frames = append(frames, nextFrame)
	}

	return frames, nil
}

// Close closes the underlying connection
func (cfr *ConnFrameReader) Close() error {
	return cfr.conn.Close()
}

// ConnFrameWriter implements FrameWriter using a buffered TCP connection
type ConnFrameWriter struct {
	conn   net.Conn
	writer *bufio.Writer
}

// NewConnFrameWriter creates a new ConnFrameWriter with specified buffer size
func NewConnFrameWriter(conn net.Conn, bufferSize int) *ConnFrameWriter {
	return &ConnFrameWriter{
		conn:   conn,
		writer: bufio.NewWriterSize(conn, bufferSize),
	}
}

// WriteFrame writes a single frame to the connection
func (tfw *ConnFrameWriter) WriteFrame(frame Frame) error {
	// Write frame length (4 bytes, big endian)
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(frame.Data)))

	_, err := tfw.writer.Write(lengthBytes)
	if err != nil {
		return FrameWriterError{Message: fmt.Sprintf("error writing frame length bytes: %v", err)}
	}

	// Write frame payload
	_, err = tfw.writer.Write(frame.Data)
	if err != nil {
		return FrameWriterError{Message: fmt.Sprintf("error writing frame data: %v", err)}
	}

	// Flush the buffer to ensure data is sent
	return tfw.writer.Flush()
}

// WriteFrames writes multiple frames to the connection
func (tfw *ConnFrameWriter) WriteFrames(frames []Frame) error {
	for _, frame := range frames {
		// Write frame length
		lengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lengthBytes, uint32(len(frame.Data)))

		_, err := tfw.writer.Write(lengthBytes)
		if err != nil {
			return FrameWriterError{Message: fmt.Sprintf("error writing frame length bytes: %v", err)}
		}

		// Write frame payload
		_, err = tfw.writer.Write(frame.Data)
		if err != nil {
			return FrameWriterError{Message: fmt.Sprintf("error writing frame data: %v", err)}
		}
	}

	// Flush all frames at once
	return tfw.writer.Flush()
}

// Close closes the underlying connection
func (tfw *ConnFrameWriter) Close() error {
	return tfw.conn.Close()
}

type FrameConn struct {
	conn   net.Conn
	reader *ConnFrameReader
	writer *ConnFrameWriter
}

// NewFrameConn creates a new FrameConn with specified buffer sizes for reading and writing
func NewFrameConn(conn net.Conn, readBufferSize, writeBufferSize int) *FrameConn {
	return &FrameConn{
		conn:   conn,
		reader: NewConnFrameReader(conn, readBufferSize),
		writer: NewConnFrameWriter(conn, writeBufferSize),
	}
}

// Reader returns the underlying FrameReader
func (fc *FrameConn) Reader() FrameReader {
	return fc.reader
}

// Writer returns the underlying FrameWriter
func (fc *FrameConn) Writer() FrameWriter {
	return fc.writer
}

// ReadFrame delegates to the reader
func (fc *FrameConn) ReadFrame() (Frame, error) {
	return fc.reader.ReadFrame()
}

// ReadFrames delegates to the reader
func (fc *FrameConn) ReadFrames() ([]Frame, error) {
	return fc.reader.ReadFrames()
}

// WriteFrame delegates to the writer
func (fc *FrameConn) WriteFrame(frame Frame) error {
	return fc.writer.WriteFrame(frame)
}

// WriteFrames delegates to the writer
func (fc *FrameConn) WriteFrames(frames []Frame) error {
	return fc.writer.WriteFrames(frames)
}

// Close closes the underlying connection
func (fc *FrameConn) Close() error {
	return fc.conn.Close()
}
