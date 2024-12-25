package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

const ErrUnsupportedVersion int16 = 35

type Message struct {
	Error         int16
	CorrelationID int32
}

func Read(c net.Conn) (*Message, error) {
	var tmp32, length int32
	var tmp16 int16
	out := Message{}
	// Header
	binary.Read(c, binary.BigEndian, &length)
	binary.Read(c, binary.BigEndian, &tmp16) // API Key
	binary.Read(c, binary.BigEndian, &tmp16) // API Version
	binary.Read(c, binary.BigEndian, &tmp32)
	out.CorrelationID = tmp32
	if tmp16 < 0 || tmp16 > 4 {
		out.Error = ErrUnsupportedVersion
	}
	// Body goes here
	tmp := make([]byte, length-8)
	c.Read(tmp)
	return &out, nil
}
func Send(c net.Conn, b []byte) error {
	binary.Write(c, binary.BigEndian, int32(len(b)))
	binary.Write(c, binary.BigEndian, b)
	return nil
}
func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	c, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	m, err := Read(c)
	if err != nil {
		fmt.Println("Error reading message: ", err.Error())
		os.Exit(1)
	}
	if m.Error != 0 {
		out := make([]byte, 6)
		binary.BigEndian.PutUint32(out, uint32(m.CorrelationID))
		binary.BigEndian.PutUint16(out[4:], uint16(m.Error))
		Send(c, out)
		os.Exit(1)
	}
	out := make([]byte, 19)
	binary.BigEndian.PutUint32(out, uint32(m.CorrelationID))
	fmt.Println("OUT -> ", out)
	binary.BigEndian.PutUint16(out[4:], 0)  // Error code
	out[6] = 2                              // Number of API keys
	binary.BigEndian.PutUint16(out[7:], 18) // API Key 1 - API_VERSIONS
	binary.BigEndian.PutUint16(out[9:], 3)  //             min version
	binary.BigEndian.PutUint16(out[11:], 4) //             max version
	out[13] = 0                             // _tagged_fields
	binary.BigEndian.PutUint32(out[14:], 0) // throttle time
	out[18] = 0
	Send(c, out)
}
