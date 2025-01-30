package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type messasgeSize struct {
	messageSize int32
}

type messageHeader struct {
	requestApiKey     int16
	requestApiVersion int16
	correlationId     int32
	clientId          []byte
	tagBuffer         []byte
}

func main() {
	fmt.Println("Logs from your program will appear here!")
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	handleConnection(conn)
}

func parseRequest(conn net.Conn, buff []byte) {
	inputRequestData := make([]byte, 1024)
	conn.Read(inputRequestData)
	binary.BigEndian.PutUint32(buff[:4], 0) // Message Size
	fmt.Println("Got -> ", binary.BigEndian.Uint32(inputRequestData[8:12]))
	binary.BigEndian.PutUint32(buff[4:8], binary.BigEndian.Uint32(inputRequestData[8:12])) //
	conn.Write(buff)
}

func handleConnection(conn net.Conn) {
	buff := make([]byte, 1024)
	parseRequest(conn, buff)
}
