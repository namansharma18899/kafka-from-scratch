package main

import "sync"

// StoredMessage holds a single message in the log for a partition
type StoredMessage struct {
	Offset int64
	Key    []byte
	Value  []byte
}

// Log is an in-memory topic partition log
type Log struct {
	mu       sync.RWMutex
	messages []StoredMessage
	nextOff  int64
}

func newLog() *Log {
	return &Log{messages: make([]StoredMessage, 0), nextOff: 0}
}

func (l *Log) append(key, value []byte) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	off := l.nextOff
	l.messages = append(l.messages, StoredMessage{Offset: off, Key: key, Value: value})
	l.nextOff++
	return off
}

func (l *Log) readFrom(offset int64, maxBytes int) ([]StoredMessage, int64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if offset >= l.nextOff || len(l.messages) == 0 {
		return nil, l.nextOff
	}
	var result []StoredMessage
	var highWaterMark int64 = l.nextOff
	n := 0
	for i := int(offset); i < len(l.messages) && n < maxBytes; i++ {
		msg := l.messages[i]
		// rough size: offset(8) + size(4) + crc(4) + magic(1) + attr(1) + key len(4) + key + value len(4) + value
		msgSize := 8 + 4 + 4 + 1 + 1 + 4 + len(msg.Key) + 4 + len(msg.Value)
		if n+msgSize > maxBytes && len(result) > 0 {
			break
		}
		result = append(result, msg)
		n += msgSize
	}
	return result, highWaterMark
}

// Broker holds in-memory topic -> partition -> log
type Broker struct {
	mu     sync.RWMutex
	logs   map[string]map[int32]*Log
	topics map[string]struct{}
}

var defaultBroker = &Broker{
	logs:   make(map[string]map[int32]*Log),
	topics: make(map[string]struct{}),
}

func (b *Broker) ensurePartition(topic string, partition int32) *Log {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.logs[topic] == nil {
		b.logs[topic] = make(map[int32]*Log)
		b.topics[topic] = struct{}{}
	}
	if b.logs[topic][partition] == nil {
		b.logs[topic][partition] = newLog()
	}
	return b.logs[topic][partition]
}

func (b *Broker) getPartition(topic string, partition int32) *Log {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.logs[topic] == nil {
		return nil
	}
	return b.logs[topic][partition]
}

func (b *Broker) listTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.topics))
	for t := range b.topics {
		names = append(names, t)
	}
	return names
}

func (b *Broker) partitionIDs(topic string) []int32 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.logs[topic] == nil {
		return nil
	}
	ids := make([]int32, 0, len(b.logs[topic]))
	for p := range b.logs[topic] {
		ids = append(ids, p)
	}
	return ids
}
