package main

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestKafkaBroker_ApiVersions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go RunWithListener(ctx, l)
	addr := l.Addr().String()
	time.Sleep(10 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	// ApiVersions request: body = ApiKey(2) + ApiVersion(2) + CorrelationId(4) + ClientId(-1)
	body := make([]byte, 12)
	binary.BigEndian.PutUint16(body[0:2], uint16(ApiKeyApiVersions))
	binary.BigEndian.PutUint16(body[2:4], 0)
	binary.BigEndian.PutUint32(body[4:8], 1)
	binary.BigEndian.PutUint16(body[8:10], 0xffff) // null client id

	req := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(req[0:4], uint32(len(body)))
	copy(req[4:], body)
	if _, err := conn.Write(req); err != nil {
		cancel()
		t.Fatal(err)
	}

	// Response: size(4) + correlationId(4) + errorCode(2) + api_versions array
	sizeBuf := make([]byte, 4)
	if _, err := conn.Read(sizeBuf); err != nil {
		cancel()
		t.Fatal(err)
	}
	respLen := int(binary.BigEndian.Uint32(sizeBuf))
	if respLen < 10 {
		cancel()
		t.Fatalf("expected response body >= 10 bytes, got %d", respLen)
	}
	resp := make([]byte, respLen)
	if _, err := conn.Read(resp); err != nil {
		cancel()
		t.Fatal(err)
	}
	corrId := binary.BigEndian.Uint32(resp[0:4])
	if corrId != 1 {
		t.Errorf("correlation id: expected 1, got %d", corrId)
	}
	errorCode := binary.BigEndian.Uint16(resp[4:6])
	if errorCode != 0 {
		t.Errorf("error code: expected 0, got %d", errorCode)
	}
	cancel()
}

func TestKafkaBroker_Metadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go RunWithListener(ctx, l)
	addr := l.Addr().String()
	time.Sleep(10 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	// Metadata request v0: ApiKey(2) + ApiVersion(2) + CorrelationId(4) + ClientId(-1) + Topics(4) = 12 + 4 = 16
	body := make([]byte, 16)
	binary.BigEndian.PutUint16(body[0:2], uint16(ApiKeyMetadata))
	binary.BigEndian.PutUint16(body[2:4], 0)
	binary.BigEndian.PutUint32(body[4:8], 2)
	binary.BigEndian.PutUint16(body[8:10], 0xffff)
	binary.BigEndian.PutUint32(body[10:14], 0) // empty topics = all

	req := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(req[0:4], uint32(len(body)))
	copy(req[4:], body)
	if _, err := conn.Write(req); err != nil {
		cancel()
		t.Fatal(err)
	}

	sizeBuf := make([]byte, 4)
	if _, err := conn.Read(sizeBuf); err != nil {
		cancel()
		t.Fatal(err)
	}
	respLen := int(binary.BigEndian.Uint32(sizeBuf))
	if respLen < 8 {
		cancel()
		t.Fatalf("expected response body >= 8 bytes, got %d", respLen)
	}
	resp := make([]byte, respLen)
	if _, err := conn.Read(resp); err != nil {
		cancel()
		t.Fatal(err)
	}
	corrId := binary.BigEndian.Uint32(resp[0:4])
	if corrId != 2 {
		t.Errorf("correlation id: expected 2, got %d", corrId)
	}
	// After correlation id: brokers array (4 bytes count) + ...
	if respLen < 12 {
		cancel()
		t.Fatalf("metadata response too short")
	}
	cancel()
}

func TestKafkaBroker_ProduceAndFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go RunWithListener(ctx, l)
	addr := l.Addr().String()
	time.Sleep(10 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	// Produce: ApiKey(2)+Version(2)+CorrId(4)+ClientId(-1) + Acks(2)+Timeout(4)+TopicCount(4)
	// + TopicName len(2)+"test"(4) + PartCount(4) + Partition(4)+RecordSetSize(4)+RecordSet
	// Minimal record set: one message offset(8)+size(4)+ [crc(4)+magic(1)+attr(1)+keyLen(4)+valueLen(4)+value]
	topic := "test"
	value := []byte("hello")
	innerLen := 4 + 1 + 1 + 4 + 4 + len(value) // crc+magic+attr+keylen(-1)+vallen+value
	recordSet := make([]byte, 0, 8+4+innerLen)
	recordSet = binary.BigEndian.AppendUint64(recordSet, 0)
	recordSet = binary.BigEndian.AppendUint32(recordSet, uint32(innerLen))
	recordSet = append(recordSet, 0, 0, 0, 0, 0, 0) // crc(4), magic(1), attr(1) = 6 bytes
	recordSet = binary.BigEndian.AppendUint32(recordSet, 0xffffffff) // key -1 (no key bytes)
	recordSet = binary.BigEndian.AppendUint32(recordSet, uint32(len(value)))
	recordSet = append(recordSet, value...)

	bodyLen := 10 + 2 + 4 + 4 + 2 + len(topic) + 4 + 4 + 4 + len(recordSet)
	body := make([]byte, 0, bodyLen)
	body = binary.BigEndian.AppendUint16(body, uint16(ApiKeyProduce))
	body = binary.BigEndian.AppendUint16(body, 0)
	body = binary.BigEndian.AppendUint32(body, 3) // corr id
	body = append(body, 0xff, 0xff)               // client id null
	body = binary.BigEndian.AppendUint16(body, 1)
	body = binary.BigEndian.AppendUint32(body, 1000)
	body = binary.BigEndian.AppendUint32(body, 1) // 1 topic
	body = binary.BigEndian.AppendUint16(body, uint16(len(topic)))
	body = append(body, topic...)
	body = binary.BigEndian.AppendUint32(body, 1) // 1 partition
	body = binary.BigEndian.AppendUint32(body, 0) // partition 0
	body = binary.BigEndian.AppendUint32(body, uint32(len(recordSet)))
	body = append(body, recordSet...)

	req := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(req[0:4], uint32(len(body)))
	copy(req[4:], body)
	if _, err := conn.Write(req); err != nil {
		cancel()
		t.Fatal(err)
	}

	sizeBuf := make([]byte, 4)
	if _, err := conn.Read(sizeBuf); err != nil {
		cancel()
		t.Fatal(err)
	}
	respLen := int(binary.BigEndian.Uint32(sizeBuf))
	resp := make([]byte, respLen)
	if _, err := conn.Read(resp); err != nil {
		cancel()
		t.Fatal(err)
	}
	if len(resp) < 12 {
		cancel()
		t.Fatalf("produce response too short")
	}
	// Response: corrId(4) + topic count(4) + topic name + partition responses
	off := binary.BigEndian.Uint64(resp[len(resp)-8:])
	if off != 0 {
		t.Errorf("expected offset 0, got %d", off)
	}

	// Fetch from offset 0
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer conn2.Close()
	conn2.SetDeadline(time.Now().Add(2 * time.Second))
	// Fetch body: ApiKey(2)+Version(2)+CorrId(4)+ClientId(-1) + ReplicaId(4)+MaxWait(4)+MinBytes(4)
	// + TopicCount(4) + TopicName(2+4)+PartCount(4)+Partition(4)+Offset(8)+MaxBytes(4)
	fetchBody := make([]byte, 0, 64)
	fetchBody = binary.BigEndian.AppendUint16(fetchBody, uint16(ApiKeyFetch))
	fetchBody = binary.BigEndian.AppendUint16(fetchBody, 0)
	fetchBody = binary.BigEndian.AppendUint32(fetchBody, 4)
	fetchBody = append(fetchBody, 0xff, 0xff)
	fetchBody = binary.BigEndian.AppendUint32(fetchBody, 0)
	fetchBody = binary.BigEndian.AppendUint32(fetchBody, 100)
	fetchBody = binary.BigEndian.AppendUint32(fetchBody, 1)
	fetchBody = binary.BigEndian.AppendUint16(fetchBody, uint16(len(topic)))
	fetchBody = append(fetchBody, topic...)
	fetchBody = binary.BigEndian.AppendUint32(fetchBody, 1)
	fetchBody = binary.BigEndian.AppendUint32(fetchBody, 0)
	fetchBody = binary.BigEndian.AppendUint64(fetchBody, 0)
	fetchBody = binary.BigEndian.AppendUint32(fetchBody, 1024)

	fetchReq := make([]byte, 4+len(fetchBody))
	binary.BigEndian.PutUint32(fetchReq[0:4], uint32(len(fetchBody)))
	copy(fetchReq[4:], fetchBody)
	if _, err := conn2.Write(fetchReq); err != nil {
		cancel()
		t.Fatal(err)
	}
	if _, err := conn2.Read(sizeBuf); err != nil {
		cancel()
		t.Fatal(err)
	}
	fetchRespLen := int(binary.BigEndian.Uint32(sizeBuf))
	fetchResp := make([]byte, fetchRespLen)
	if _, err := conn2.Read(fetchResp); err != nil {
		cancel()
		t.Fatal(err)
	}
	// Response: correlation id (4) + topic count (4) + at least one topic with partition data
	if fetchRespLen < 8 {
		cancel()
		t.Fatalf("fetch response too short: %d", fetchRespLen)
	}
	corrId := binary.BigEndian.Uint32(fetchResp[0:4])
	if corrId != 4 {
		t.Errorf("fetch correlation id: expected 4, got %d", corrId)
	}
	if fetchRespLen >= 12 {
		topicCount := binary.BigEndian.Uint32(fetchResp[8:12])
		if topicCount != 1 {
			t.Errorf("fetch topic count: expected 1, got %d", topicCount)
		}
	}
	cancel()
}
