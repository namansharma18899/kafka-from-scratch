package main

import (
	"bytes"
	"encoding/binary"
)

// Supported APIs we advertise in ApiVersions: ApiKey -> [MinVersion, MaxVersion]
var supportedAPIs = []struct {
	apiKey     int16
	minVersion int16
	maxVersion int16
}{
	{ApiKeyProduce, 0, 9},
	{ApiKeyFetch, 0, 13},
	{ApiKeyListOffsets, 0, 7},
	{ApiKeyMetadata, 0, 12},
	{ApiKeyApiVersions, 0, 3},
}

func handleApiVersions(body []byte, correlationId int32) []byte {
	// ApiVersions response: ErrorCode(2) + ApiVersions array (int32 count + entries)
	var buf bytes.Buffer
	tmp := make([]byte, 2)
	binary.BigEndian.PutUint16(tmp, 0)
	buf.Write(tmp) // error code 0
	arrLen := make([]byte, 4)
	binary.BigEndian.PutUint32(arrLen, uint32(len(supportedAPIs)))
	buf.Write(arrLen)
	for _, api := range supportedAPIs {
		binary.BigEndian.PutUint16(tmp, uint16(api.apiKey))
		buf.Write(tmp)
		binary.BigEndian.PutUint16(tmp, uint16(api.minVersion))
		buf.Write(tmp)
		binary.BigEndian.PutUint16(tmp, uint16(api.maxVersion))
		buf.Write(tmp)
	}
	return buildResponse(correlationId, buf.Bytes())
}

func buildResponse(correlationId int32, payload []byte) []byte {
	// Response: CorrelationId(4) + payload
	b := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(b[0:4], uint32(correlationId))
	copy(b[4:], payload)
	return b
}

func handleMetadata(body []byte, correlationId int32) []byte {
	// Metadata request v0: Topics array (int32 count, then each topic name string)
	pos := requestBodyOffset(body)
	if pos+4 > len(body) {
		return buildResponse(correlationId, buildMetadataResponse(nil))
	}
	topicCount := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	var topics []string
	for i := int32(0); i < topicCount && pos+2 <= len(body); i++ {
		strLen := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
		if strLen < 0 || pos+int(strLen) > len(body) {
			break
		}
		topics = append(topics, string(body[pos:pos+int(strLen)]))
		pos += int(strLen)
	}
	return buildResponse(correlationId, buildMetadataResponse(topics))
}

func buildMetadataResponse(requestedTopics []string) []byte {
	var buf bytes.Buffer
	// Brokers array: int32 count, then each: NodeId(4), Host(string int16 len + bytes), Port(4)
	buf.Write([]byte{0, 0, 0, 1}) // 1 broker
	buf.Write([]byte{0, 0, 0, 0}) // node_id 0
	host := []byte("localhost")
	buf.Write([]byte{0, 9}) // host string length 9
	buf.Write(host)
	buf.Write([]byte{0, 0, 0x23, 0x84}) // port 9092
	// TopicMetadata array
	topics := defaultBroker.listTopics()
	if len(requestedTopics) > 0 {
		topics = requestedTopics
	}
	topicMetaLen := make([]byte, 4)
	binary.BigEndian.PutUint32(topicMetaLen, uint32(len(topics)))
	buf.Write(topicMetaLen)
	for _, topic := range topics {
		buf.Write([]byte{0, 0}) // error code 0
		buf.Write(encodeString(topic))
		partitions := defaultBroker.partitionIDs(topic)
		if len(partitions) == 0 {
			partitions = []int32{0}
		}
		partMetaLen := make([]byte, 4)
		binary.BigEndian.PutUint32(partMetaLen, uint32(len(partitions)))
		buf.Write(partMetaLen)
		for _, pID := range partitions {
			buf.Write([]byte{0, 0}) // error code
			pidBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(pidBytes, uint32(pID))
			buf.Write(pidBytes)           // partition id
			buf.Write([]byte{0, 0, 0, 0}) // leader id 0
			buf.Write([]byte{0, 0, 0, 1}) // replicas count 1
			buf.Write(pidBytes)           // replica 0
			buf.Write([]byte{0, 0, 0, 1}) // isr count 1
			buf.Write(pidBytes)
		}
	}
	return buf.Bytes()
}

// handleProduce parses Produce request v0 and appends to log
func handleProduce(body []byte, correlationId int32) []byte {
	// Produce v0: Acks(2), Timeout(4), TopicCount(4), then per topic: TopicName, PartitionCount(4), per partition: Partition(4), RecordSetSize(4), RecordSet(bytes)
	pos := requestBodyOffset(body)
	if pos+10 > len(body) {
		return buildResponse(correlationId, buildProduceResponse(nil))
	}
	acks := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
	_ = acks
	pos += 2
	pos += 4 // timeout
	if pos+4 > len(body) {
		return buildResponse(correlationId, buildProduceResponse(nil))
	}
	topicCount := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	var responses []struct {
		topic     string
		partition int32
		offset    int64
		err       int16
	}
	for t := int32(0); t < topicCount && pos+2 <= len(body); t++ {
		topicLen := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
		if topicLen < 0 || pos+int(topicLen) > len(body) {
			break
		}
		topic := string(body[pos : pos+int(topicLen)])
		pos += int(topicLen)
		if pos+4 > len(body) {
			break
		}
		partCount := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		for p := int32(0); p < partCount && pos+8 <= len(body); p++ {
			partition := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
			pos += 4
			recordSetSize := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
			pos += 4
			if recordSetSize < 0 || pos+int(recordSetSize) > len(body) {
				break
			}
			recordSet := body[pos : pos+int(recordSetSize)]
			pos += int(recordSetSize)
			offset, errCode := appendMessageSet(topic, partition, recordSet)
			responses = append(responses, struct {
				topic     string
				partition int32
				offset    int64
				err       int16
			}{topic, partition, offset, errCode})
		}
	}
	return buildResponse(correlationId, buildProduceResponse(responses))
}

func appendMessageSet(topic string, partition int32, recordSet []byte) (offset int64, errCode int16) {
	// Legacy message set: each message is Offset(8), MessageSize(4), CRC(4), Magic(1), Attr(1), Key(-1 or len+bytes), Value(len+bytes)
	// We only support simple format: offset, size, then inner message
	pos := 0
	var lastOffset int64 = -1
	for pos+12 <= len(recordSet) {
		_ = int64(binary.BigEndian.Uint64(recordSet[pos : pos+8])) // offset in message set
		msgSize := int32(binary.BigEndian.Uint32(recordSet[pos+8 : pos+12]))
		pos += 12
		if msgSize <= 0 || pos+int(msgSize) > len(recordSet) {
			break
		}
		msg := recordSet[pos : pos+int(msgSize)]
		pos += int(msgSize)
		// msg: CRC(4), Magic(1), Attr(1), Key(4+bytes), Value(4+bytes)
		if len(msg) < 6 {
			continue
		}
		keyLen := int32(binary.BigEndian.Uint32(msg[6:10]))
		var key, value []byte
		cur := 10
		if keyLen >= 0 && cur+int(keyLen) <= len(msg) {
			key = msg[cur : cur+int(keyLen)]
			cur += int(keyLen)
		}
		if cur+4 <= len(msg) {
			valueLen := int32(binary.BigEndian.Uint32(msg[cur : cur+4]))
			cur += 4
			if valueLen >= 0 && cur+int(valueLen) <= len(msg) {
				value = msg[cur : cur+int(valueLen)]
			}
		}
		log := defaultBroker.ensurePartition(topic, partition)
		lastOffset = log.append(key, value)
	}
	if lastOffset < 0 {
		return 0, 0
	}
	return lastOffset, 0
}

func buildProduceResponse(responses []struct {
	topic     string
	partition int32
	offset    int64
	err       int16
}) []byte {
	var buf bytes.Buffer
	// Produce response v0: no throttle; v1+ has throttle_time_ms
	// Group by topic for response
	type partResp struct {
		partition int32
		offset    int64
		err       int16
	}
	byTopic := make(map[string][]partResp)
	for _, r := range responses {
		byTopic[r.topic] = append(byTopic[r.topic], partResp{r.partition, r.offset, r.err})
	}
	count := make([]byte, 4)
	binary.BigEndian.PutUint32(count, uint32(len(byTopic)))
	buf.Write(count)
	for topic, parts := range byTopic {
		buf.Write(encodeString(topic))
		binary.BigEndian.PutUint32(count, uint32(len(parts)))
		buf.Write(count)
		for _, p := range parts {
			partBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partBytes, uint32(p.partition))
			buf.Write(partBytes)
			buf.Write([]byte{0, 0}) // error code
			offBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(offBytes, uint64(p.offset))
			buf.Write(offBytes)
		}
	}
	return buf.Bytes()
}

func handleFetch(body []byte, correlationId int32) []byte {
	// Fetch v0: ReplicaId(4), MaxWait(4), MinBytes(4), Topics array
	pos := requestBodyOffset(body)
	if pos+12 > len(body) {
		return buildResponse(correlationId, buildFetchResponse(nil))
	}
	pos += 12 // replica_id, max_wait, min_bytes
	if pos+4 > len(body) {
		return buildResponse(correlationId, buildFetchResponse(nil))
	}
	topicCount := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
	pos += 4
	var fetches []struct {
		topic     string
		partition int32
		offset    int64
		maxBytes  int32
	}
	for t := int32(0); t < topicCount && pos+2 <= len(body); t++ {
		topicLen := int16(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
		if topicLen < 0 || pos+int(topicLen) > len(body) {
			break
		}
		topic := string(body[pos : pos+int(topicLen)])
		pos += int(topicLen)
		if pos+4 > len(body) {
			break
		}
		partCount := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		for p := int32(0); p < partCount && pos+20 <= len(body); p++ {
			partition := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
			pos += 4
			offset := int64(binary.BigEndian.Uint64(body[pos : pos+8]))
			pos += 8
			maxBytes := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
			pos += 4
			fetches = append(fetches, struct {
				topic     string
				partition int32
				offset    int64
				maxBytes  int32
			}{topic, partition, offset, maxBytes})
		}
	}
	return buildResponse(correlationId, buildFetchResponse(fetches))
}

func buildFetchResponse(fetches []struct {
	topic     string
	partition int32
	offset    int64
	maxBytes  int32
}) []byte {
	// Group by topic
	type partFetch struct {
		partition int32
		offset    int64
		maxBytes  int32
	}
	byTopic := make(map[string][]partFetch)
	for _, f := range fetches {
		byTopic[f.topic] = append(byTopic[f.topic], partFetch{f.partition, f.offset, f.maxBytes})
	}
	var buf bytes.Buffer
	count := make([]byte, 4)
	binary.BigEndian.PutUint32(count, uint32(len(byTopic)))
	buf.Write(count)
	for topic, parts := range byTopic {
		buf.Write(encodeString(topic))
		binary.BigEndian.PutUint32(count, uint32(len(parts)))
		buf.Write(count)
		for _, f := range parts {
			partBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partBytes, uint32(f.partition))
			buf.Write(partBytes)
			buf.Write([]byte{0, 0}) // error code
			if log := defaultBroker.getPartition(topic, f.partition); log != nil {
				msgs, highWaterMark := log.readFrom(f.offset, int(f.maxBytes))
				msgSet := encodeMessageSet(msgs)
				hwBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(hwBytes, uint64(highWaterMark))
				buf.Write(hwBytes)
				msgSize := make([]byte, 4)
				binary.BigEndian.PutUint32(msgSize, uint32(len(msgSet)))
				buf.Write(msgSize)
				buf.Write(msgSet)
			} else {
				buf.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0}) // high water 0
				buf.Write([]byte{0, 0, 0, 0})             // message set size 0
			}
		}
	}
	return buf.Bytes()
}

func encodeMessageSet(msgs []StoredMessage) []byte {
	out := make([]byte, 0, 1024)
	for _, m := range msgs {
		innerLen := 4 + 1 + 1 + 4 + len(m.Key) + 4 + len(m.Value)
		off := make([]byte, 8)
		binary.BigEndian.PutUint64(off, uint64(m.Offset))
		out = append(out, off...)
		size := make([]byte, 4)
		binary.BigEndian.PutUint32(size, uint32(innerLen))
		out = append(out, size...)
		out = append(out, 0, 0, 0, 0) // CRC
		out = append(out, 0)          // magic
		out = append(out, 0)          // attr
		keyLen := make([]byte, 4)
		binary.BigEndian.PutUint32(keyLen, uint32(len(m.Key)))
		out = append(out, keyLen...)
		out = append(out, m.Key...)
		valLen := make([]byte, 4)
		binary.BigEndian.PutUint32(valLen, uint32(len(m.Value)))
		out = append(out, valLen...)
		out = append(out, m.Value...)
	}
	return out
}
