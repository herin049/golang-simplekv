package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
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

type FrameReadWriter interface {
	FrameReader
	FrameWriter
}

type FrameWriterError struct {
	Message string
}

func (e FrameWriterError) Error() string {
	return e.Message
}

type BufferedFrameReader struct {
	reader       *bufio.Reader
	maxFrameSize uint32
}

func NewBufferedFrameReader(reader io.Reader, bufferSize int) *BufferedFrameReader {
	return &BufferedFrameReader{
		reader:       bufio.NewReaderSize(reader, bufferSize),
		maxFrameSize: 1024 * 1024,
	}
}

func (cfr *BufferedFrameReader) ReadFrame() (Frame, error) {
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
func (cfr *BufferedFrameReader) ReadFrames() ([]Frame, error) {
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

// BufferedFrameWriter implements FrameWriter using a buffered TCP connection
type BufferedFrameWriter struct {
	writer *bufio.Writer
}

// NewBufferedFrameWriter creates a new BufferedFrameWriter with specified buffer size
func NewBufferedFrameWriter(writer io.Writer, bufferSize int) *BufferedFrameWriter {
	return &BufferedFrameWriter{
		writer: bufio.NewWriterSize(writer, bufferSize),
	}
}

// WriteFrame writes a single frame to the connection
func (tfw *BufferedFrameWriter) WriteFrame(frame Frame) error {
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
func (tfw *BufferedFrameWriter) WriteFrames(frames []Frame) error {
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

type BufferedFrameReadWriter struct {
	reader *BufferedFrameReader
	writer *BufferedFrameWriter
}

// NewBufferedFrameIo creates a new BufferedFrameReadWriter with specified buffer sizes for reading and writing
func NewBufferedFrameIo(rw io.ReadWriter, readBufferSize, writeBufferSize int) *BufferedFrameReadWriter {
	return &BufferedFrameReadWriter{
		reader: NewBufferedFrameReader(rw, readBufferSize),
		writer: NewBufferedFrameWriter(rw, writeBufferSize),
	}
}

// Reader returns the underlying FrameReader
func (fc *BufferedFrameReadWriter) Reader() FrameReader {
	return fc.reader
}

// Writer returns the underlying FrameWriter
func (fc *BufferedFrameReadWriter) Writer() FrameWriter {
	return fc.writer
}

// ReadFrame delegates to the reader
func (fc *BufferedFrameReadWriter) ReadFrame() (Frame, error) {
	return fc.reader.ReadFrame()
}

// ReadFrames delegates to the reader
func (fc *BufferedFrameReadWriter) ReadFrames() ([]Frame, error) {
	return fc.reader.ReadFrames()
}

// WriteFrame delegates to the writer
func (fc *BufferedFrameReadWriter) WriteFrame(frame Frame) error {
	return fc.writer.WriteFrame(frame)
}

// WriteFrames delegates to the writer
func (fc *BufferedFrameReadWriter) WriteFrames(frames []Frame) error {
	return fc.writer.WriteFrames(frames)
}
