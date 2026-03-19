package main

import (
	"encoding/binary"
	"io"
)

// Kafka API keys (ApiKeys enum in Kafka protocol)
const (
	ApiKeyProduce     = 0
	ApiKeyFetch       = 1
	ApiKeyListOffsets = 2
	ApiKeyMetadata    = 3
	ApiKeyApiVersions = 18
)

// Request header (after the 4-byte size field)
// Size(4) + ApiKey(2) + ApiVersion(2) + CorrelationId(4) + ClientId(string)
func readRequestHeader(r io.Reader) (apiKey int16, apiVersion int16, correlationId int32, body []byte, err error) {
	var sizeBuf [4]byte
	if _, err = io.ReadFull(r, sizeBuf[:]); err != nil {
		return 0, 0, 0, nil, err
	}
	bodyLen := int(binary.BigEndian.Uint32(sizeBuf[:]))
	if bodyLen <= 0 || bodyLen > 1024*1024 {
		body = make([]byte, 0)
		return 0, 0, 0, body, nil
	}
	body = make([]byte, bodyLen)
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, 0, 0, nil, err
	}
	if bodyLen < 8 {
		return 0, 0, 0, body, nil
	}
	apiKey = int16(binary.BigEndian.Uint16(body[0:2]))
	apiVersion = int16(binary.BigEndian.Uint16(body[2:4]))
	correlationId = int32(binary.BigEndian.Uint32(body[4:8]))
	// ClientId is nullable string: int16 length, then bytes. -1 = null
	pos := 8
	if pos+2 <= bodyLen {
		clientIdLen := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
		if clientIdLen >= 0 && pos+int(clientIdLen) <= bodyLen {
			pos += int(clientIdLen)
		}
	}
	return apiKey, apiVersion, correlationId, body, nil
}

// RequestBodyOffset returns the byte offset in body where API-specific data starts (after ApiKey, ApiVersion, CorrelationId, ClientId).
func requestBodyOffset(body []byte) int {
	if len(body) < 10 {
		return len(body)
	}
	clientIdLen := int16(binary.BigEndian.Uint16(body[8:10]))
	if clientIdLen < 0 {
		return 10
	}
	return 10 + int(clientIdLen)
}

func putInt16(b []byte, v int16) {
	binary.BigEndian.PutUint16(b, uint16(v))
}

func putInt32(b []byte, v int32) {
	binary.BigEndian.PutUint32(b, uint32(v))
}

func putInt64(b []byte, v int64) {
	binary.BigEndian.PutUint64(b, uint64(v))
}

// Write response frame: 4-byte size (length of response body) + response body
func writeResponse(w io.Writer, correlationId int32, body []byte) error {
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(body)))
	if _, err := w.Write(sizeBuf); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// Nullable string: int16 length (-1 = null) + bytes
func encodeNullableString(s []byte) []byte {
	if s == nil {
		return []byte{0xff, 0xff} // -1
	}
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b[0:2], uint16(len(s)))
	copy(b[2:], s)
	return b
}

func encodeString(s string) []byte {
	return encodeNullableString([]byte(s))
}
