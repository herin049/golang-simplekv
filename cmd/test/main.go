package main

import (
	"errors"
	"fmt"
	"io"
	net2 "lukas/simplekv/internal/msg"
	"net"
	"time"
)

// Example usage demonstrating the frame connection
func ExampleUsage() {
	// Server example
	go func() {
		listener, err := net.Listen("tcp", ":8080")
		if err != nil {
			panic(err)
		}
		defer listener.Close()

		fmt.Println("Server listening on :8080")

		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}

			go handleConnection(conn)
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Client example
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Create frame connection with custom buffer sizes
	frameConn := net2.NewBufferedFrameIo(conn, 8192, 4096) // 8KB read buffer, 4KB write buffer

	// Example 1: Send multiple frames at once
	testFrames := []net2.Frame{
		{Data: []byte("Hello")},
		{Data: []byte("World")},
		{Data: []byte("Multiple frames test")},
	}

	fmt.Println("Sending batch of frames...")
	err = frameConn.WriteFrames(testFrames)
	if err != nil {
		fmt.Printf("Error writing frames: %v\n", err)
		return
	}

	// Example 2: Send a single frame
	err = frameConn.WriteFrame(net2.Frame{Data: []byte("Single frame msg")})
	if err != nil {
		fmt.Printf("Error writing single frame: %v\n", err)
		return
	}

	// Example 3: Read responses
	fmt.Println("Reading responses...")
	for i := 0; i < 2; i++ { // Expect 2 responses (batch + single)
		frames, err := frameConn.ReadFrames()
		if err != nil {
			fmt.Printf("Error reading frames: %v\n", err)
			break
		}

		fmt.Printf("Received %d frames in batch %d:\n", len(frames), i+1)
		for j, frame := range frames {
			fmt.Printf("  Frame %d: %s\n", j+1, string(frame.Data))
		}
	}

	fmt.Println("Client completed successfully!")
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Create separate reader and writer for more control
	reader := net2.NewBufferedFrameReader(conn, 64000) // 16KB read buffer
	writer := net2.NewBufferedFrameWriter(conn, 8192)  // 8KB write buffer

	// Or use the combined BufferedFrameReadWriter
	// frameConn := NewBufferedFrameIo(conn, 16384, 8192)

	fmt.Println("New connection established")

	for {
		// Read frames (blocks until at least one is available, then reads all available)
		frames, err := reader.ReadFrames()
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Connection closed by client")
			} else {
				fmt.Printf("Error reading frames: %v\n", err)
			}
			break
		}

		fmt.Printf("Server received %d frames:\n", len(frames))
		for i, frame := range frames {
			fmt.Printf("  Frame %d: %s\n", i+1, string(frame.Data))
		}

		// Echo frames back to client
		err = writer.WriteFrames(frames)
		if err != nil {
			fmt.Printf("Error echoing frames: %v\n", err)
			break
		}
	}
}

// Advanced usage example showing different patterns
func AdvancedUsageExample() {
	go func() {
		listener, err := net.Listen("tcp", ":8080")
		if err != nil {
			panic(err)
		}
		defer listener.Close()

		fmt.Println("Server listening on :8080")

		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}

			go handleConnection(conn)
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Error connecting to server: %v\n", err)
		return
	}
	defer conn.Close()

	frameConn := net2.NewBufferedFrameIo(conn, 8192, 4096)

	// Pattern 1: High-throughput batch processing
	batchSize := 100
	//frames := make([]store.Frame, batchSize)
	for i := 0; i < batchSize; i++ {
		fmt.Printf("Sending frame %d\n", i)
		frame := net2.Frame{Data: []byte(fmt.Sprintf("Batch msg %d", i))}
		err := frameConn.WriteFrame(frame)
		if err != nil {
			fmt.Printf("Error writing frames: %v\n", err)
			return
		}
	}

	//fmt.Printf("Sending %d frames in batch...\n", batchSize)
	//err = frameConn.WriteFrames(frames)
	//if err != nil {
	//	fmt.Printf("Batch write error: %v\n", err)
	//	return
	//}

	// Pattern 2: Request-response pattern
	request := net2.Frame{Data: []byte("PING")}
	err = frameConn.WriteFrame(request)
	if err != nil {
		fmt.Printf("Request write error: %v\n", err)
		return
	}

	response, err := frameConn.ReadFrame()
	if err != nil {
		fmt.Printf("Response read error: %v\n", err)
		return
	}
	fmt.Printf("Response: %s\n", string(response.Data))

	// Pattern 3: Streaming with flow control
	for i := 0; i < 10; i++ {
		frame := net2.Frame{Data: []byte(fmt.Sprintf("Stream msg %d", i))}
		err = frameConn.WriteFrame(frame)
		if err != nil {
			break
		}

		// Read acknowledgment
		ack, err := frameConn.ReadFrame()
		if err != nil {
			break
		}
		fmt.Printf("Received ACK: %s\n", string(ack.Data))
	}
}

func main() {
	AdvancedUsageExample()
}
