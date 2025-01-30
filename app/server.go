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

var SupportedApiVersions = []int32{0, 1, 2, 3, 4}

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

func validateApiVersion(inputBuffer []byte, responseBuffer []byte) int {
	requestApiVersion := binary.BigEndian.Uint16(inputBuffer[6:8]) // api version
	for val, _ := range SupportedApiVersions {
		if int(requestApiVersion) == int(val) {
			return 0
		}
	}
	return 35 // unsupporered version
}

func parseRequest(conn net.Conn, buff []byte) {
	inputRequestData := make([]byte, 1024)
	conn.Read(inputRequestData)
	binary.BigEndian.PutUint32(buff[:4], 0)                                                    // Message Size
	binary.BigEndian.PutUint32(buff[4:8], binary.BigEndian.Uint32(inputRequestData[8:12]))     //
	binary.BigEndian.PutUint16(buff[8:10], uint16(validateApiVersion(inputRequestData, buff))) // Api Version
	conn.Write(buff)
}

func handleConnection(conn net.Conn) {
	buff := make([]byte, 1024)
	parseRequest(conn, buff)
}
