# Kafka-from-scratch — Features & TODO

## Implemented

### Kafka protocol / APIs

| Feature | Status | Notes |
|---------|--------|-------|
| Request framing | Done | 4-byte size + body; parse ApiKey, ApiVersion, CorrelationId, ClientId |
| ApiVersions (18) | Done | Full response: error code + array of (ApiKey, MinVersion, MaxVersion); advertises Produce, Fetch, ListOffsets, Metadata, ApiVersions |
| Metadata (3) | Done | v0: request topics (empty = all); response: 1 broker (localhost:9092), topic + partition metadata (leader, replicas, ISR) |
| Produce (0) | Done | v0: Acks, Timeout, topic/partition/record set; legacy message set only (offset, size, CRC, magic, attr, key, value) |
| Fetch (1) | Done | v0: ReplicaId, MaxWait, MinBytes, topic/partition/offset/maxBytes; response: high watermark + legacy message set |

### Storage & runtime

| Feature | Status | Notes |
|---------|--------|-------|
| In-memory log | Done | Per-topic, per-partition Log with append and read-from-offset |
| Single broker | Done | One process, one broker (node_id=0), no cluster |
| Concurrent connections | Done | One goroutine per connection; request loop per connection |
| Graceful shutdown | Done | SIGINT/SIGTERM → stop accept, drain connections (5s timeout) |
| Configurable port | Done | PORT env (default 9092); script uses 9093 locally |

### Testing & docs

| Feature | Status | Notes |
|---------|--------|-------|
| Integration tests | Done | ApiVersions, Metadata, Produce+Fetch (in-process broker) |
| README | Done | Run broker, run tests, graceful shutdown |

---

## Not implemented (missing)

### APIs

| Feature | Status | Notes |
|---------|--------|-------|
| ListOffsets (2) | Missing | Advertised in ApiVersions; no handler (falls to default empty response) |
| Produce v1+ | Missing | Only v0 (no throttle_time_ms in response, etc.) |
| Fetch v1+ | Missing | Only v0 (no throttle, no log_start_offset, etc.) |
| Metadata v1+ | Missing | No cluster_id, throttle_time_ms |
| Other APIs | Missing | e.g. FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, DescribeGroups, DescribeConfigs, CreateTopics, DeleteTopics, etc. |
