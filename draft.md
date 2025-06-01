# zeroio Protocol V1

> Note: All multi-byte numeric values in this protocol are encoded in network byte order (big-endian).
> All structured data (Header and Payload) uses MessagePack format for serialization.

## Base Format

| Field         | Type   | Size (bytes) | Description                                                |
| ------------- | ------ | ------------ | ---------------------------------------------------------- |
| Version       | uint8  | 1            | Protocol version (1)                                       |
| Type          | uint8  | 1            | Message type (0-7)                                         |
| ClientID      | uint32 | 4            | Unique client identifier (32-bit big-endian)               |
| Reserved      | bytes  | 16           | Reserved bytes for future use (must be zero)               |
| HeaderLength  | uint32 | 4            | Length of the header section in bytes (32-bit big-endian)  |
| Header        | bytes  | variable     | Variable length header data (MessagePack encoded)          |
| PayloadLength | uint64 | 8            | Length of the payload section in bytes (64-bit big-endian) |
| Payload       | bytes  | variable     | Variable length payload data (MessagePack encoded)         |

**Total base header size: 34 bytes (26 + 8 for payload length)**

## Message Types

| Type  | Name         | Value | Description                             |
| ----- | ------------ | ----- | --------------------------------------- |
| JOIN  | Join         | 0     | Join a group/connect to broker          |
| REQ   | Request      | 1     | Send a request to single client         |
| REP   | Response     | 2     | Send a response to a request            |
| NOTIF | Notification | 3     | Send a notification to specific clients |
| BCAST | Broadcast    | 4     | Send a broadcast to all clients         |
| PUB   | Topic        | 5     | Publish a message to a topic            |
| SUB   | Subscribe    | 6     | Subscribe to a topic                    |
| UNSUB | Unsubscribe  | 7     | Unsubscribe from a topic                |
| PING  | Ping         | 8     | Connection keep-alive ping              |
| PONG  | Pong         | 9     | Connection keep-alive pong              |

## Header Structure

The header is a MessagePack encoded structure with the following optional fields:

```
Header Map:
  "routing" => Array of Routing Objects (optional)
  "reqrep" => Request/Response Object (optional)
  "topic" => String (optional)
  "status" => Integer (optional)
  "auth" => Authentication Object (optional)
  "keepalive" => Keep-alive Object (optional)

Routing Object:
  "client_id" => 32-bit unsigned integer
  "path" => String

Request/Response Object:
  "type" => String ("request" or "correlation")
  "id" => String (unique identifier)

Authentication Object:
  "type" => String ("token", "basic", or "api_key")
  "token" => String (for token auth)
  "username" => String (for basic auth)
  "password" => String (for basic auth)
  "api_key" => String (for API key auth)

Keep-alive Object:
  "timestamp" => 64-bit unsigned integer (Unix timestamp in milliseconds)
  "interval" => 32-bit unsigned integer (suggested interval in seconds, optional)
```

### Routing Format

```json
{
  "client_id": 1234,
  "path": "/api/endpoint"
}
```

- `client_id`: Target client identifier (32-bit unsigned integer)
- `path`: Routing path string for the target endpoint

### Request/Response Correlation

```json
{
  "type": "request",
  "id": "01H2XQMGPE7ZPXXNKQD9XE8S6R"
}
```

```json
{
  "type": "correlation",
  "id": "01H2XQMGPE7ZPXXNKQD9XE8S6R"
}
```

- For REQ messages: `type` must be "request"
- For REP messages: `type` must be "correlation" and `id` must match the original request
- `id`: Unique identifier for correlation (recommended: ULID)

### Header Field Requirements

| Message Type | Required Fields | Optional Fields   | Forbidden Fields                         |
| ------------ | --------------- | ----------------- | ---------------------------------------- |
| JOIN         | -               | auth              | routing, reqrep, topic, keepalive       |
| REQ          | routing, reqrep | status            | topic, auth, keepalive                   |
| REP          | routing, reqrep | status            | topic, auth, keepalive                   |
| NOTIF        | routing         | status            | reqrep, topic, auth, keepalive           |
| BCAST        | -               | status            | routing, reqrep, topic, auth, keepalive  |
| PUB          | topic           | routing, status   | reqrep, auth, keepalive                  |
| SUB          | topic           | -                 | routing, reqrep, status, auth, keepalive |
| UNSUB        | topic           | -                 | routing, reqrep, status, auth, keepalive |
| PING         | keepalive       | -                 | routing, reqrep, topic, status, auth     |
| PONG         | keepalive       | -                 | routing, reqrep, topic, status, auth     |

## Status Codes

The protocol uses HTTP-like status codes:

### Success Codes (200-299)

- **200 OK**: Request completed successfully
- **201 Created**: Resource was created successfully
- **202 Accepted**: Request accepted for processing

### Client Error Codes (400-499)

- **400 Bad Request**: Invalid request format or parameters
- **401 Unauthorized**: Authentication required
- **403 Forbidden**: Access denied
- **404 Not Found**: Resource not found
- **405 Method Not Allowed**: Operation not supported
- **408 Request Timeout**: Request timed out

### Server Error Codes (500-599)

- **500 Internal Server Error**: Server encountered an error
- **501 Not Implemented**: Feature not implemented
- **502 Bad Gateway**: Invalid response from upstream
- **503 Service Unavailable**: Service temporarily unavailable
- **504 Gateway Timeout**: Upstream timeout

### Protocol Specific Codes (600-699)

- **600 Client Not Found**: Target client does not exist
- **601 Topic Not Found**: Specified topic does not exist
- **602 Invalid Routing**: Routing information is invalid
- **603 Subscription Failed**: Failed to subscribe to topic

## Authentication Types

### Token Authentication

- `type`: "token"
- `token`: Bearer token string

### Basic Authentication

- `type`: "basic"
- `username`: Username string
- `password`: Password string

### API Key Authentication

- `type`: "api_key"
- `api_key`: API key string

## Header Examples

### Join Message

**Without Authentication:**

- Empty header map

**With Token Authentication:**

- auth.type = "token"
- auth.token = "authentication-token"

### Request Message

- routing[0].client_id = 1234
- routing[0].path = "/api/users"
- reqrep.type = "request"
- reqrep.id = "unique-request-id"

### Response Message

- routing[0].client_id = 5678
- routing[0].path = "/api/users"
- reqrep.type = "correlation"
- reqrep.id = "unique-request-id"
- status = 200

### Topic Message

- topic = "news_updates"

### Subscribe Message

- topic = "news_updates"

### Notification Message

- routing[0].client_id = 1234
- routing[0].path = "/notifications"
- routing[1].client_id = 5678
- routing[1].path = "/alerts"

### Ping Message

- keepalive.timestamp = 1703123456789
- keepalive.interval = 30

### Pong Message

- keepalive.timestamp = 1703123456789

## Connection Keep-alive

The protocol includes a built-in keep-alive mechanism using PING and PONG messages to maintain connection health and detect disconnections.

### Keep-alive Mechanism

- **PING Messages**: Sent by either party to test connection liveness
- **PONG Messages**: Sent in response to PING messages to confirm connection health
- **Timestamp**: Unix timestamp in milliseconds when the message was created
- **Interval**: Suggested keep-alive interval in seconds (optional, only in PING)

### Keep-alive Rules

1. **PING Initiation**: Either client or broker can initiate keep-alive by sending PING
2. **PONG Response**: Recipient MUST respond with PONG containing the same timestamp
3. **Timeout Detection**: Sender should consider connection dead if no PONG received within reasonable timeout
4. **Interval Suggestion**: PING can include suggested interval for subsequent keep-alives
5. **Automatic Keep-alive**: Implementations MAY automatically send PING messages at regular intervals

### Keep-alive Examples

**PING Message:**
```
Header: {
  "keepalive": {
    "timestamp": 1703123456789,
    "interval": 30
  }
}
Payload: (empty)
```

**PONG Response:**
```
Header: {
  "keepalive": {
    "timestamp": 1703123456789
  }
}
Payload: (empty)
```

### Keep-alive Best Practices

- Default keep-alive interval: 30 seconds
- PONG timeout: 3x keep-alive interval
- Exponential backoff for failed connections
- Graceful degradation on keep-alive failures

## Transport Support

| Transport | URL Scheme | Description                 | Connection Initiator | ClientID Assignment |
| --------- | ---------- | --------------------------- | -------------------- | ------------------- |
| TCP       | tcp://     | TCP socket connection       | Client               | Broker              |
| IPC       | ipc://     | Inter-process communication | Client               | Broker              |
| WebSocket | ws://      | WebSocket connection        | Client               | Broker              |
| STDIO     | stdio://   | Standard input/output pipes | Broker               | Broker              |

### Transport URLs

- **TCP**: `tcp://hostname:port` (e.g., `tcp://localhost:8080`)
- **IPC**: `ipc:///path/to/socket` (e.g., `ipc:///tmp/zeroio.sock`)
- **WebSocket**: `ws://hostname:port/path` (e.g., `ws://localhost:8080/ws`)
- **STDIO**: `stdio://` (standard input/output pipes)

### STDIO Transport Details

The STDIO transport is specifically designed for subprocess communication where the broker launches client processes:

- **Connection Initiator**: Always the broker (launches subprocess)
- **Communication Channel**: Parent-child process pipes (stdin/stdout)
- **Use Case**: Broker spawns worker processes or specialized handlers
- **Process Lifecycle**: Managed by broker, terminated on disconnect
- **Security**: Inherits broker's process permissions and environment

## ClientID Assignment and Handshake

### ClientID Values

The protocol defines special ClientID values for system operations:

- **0 (UNASSIGNED)**: Used by clients in JOIN messages before broker assignment
- **1 (BROKER)**: Reserved for broker internal operations
- **1000-4294967294**: Valid range for client assignment by broker
- **4294967295**: Reserved for future use

### Join Handshake Process

1. **Initial JOIN Message**: Client sends JOIN message with ClientID = 0 (UNASSIGNED)
   - Binary: `0x00 0x00 0x00 0x00` at bytes 2-5 in base header
2. **Authentication**: Broker validates optional authentication credentials
3. **ClientID Assignment**: Broker assigns unique ClientID from valid range (≥1000)
4. **Response**: Broker sends response with assigned ClientID in base header
   - Example: For ClientID 1001, response contains `0x00 0x00 0x03 0xE9` at bytes 2-5
5. **Confirmation**: All subsequent messages use the assigned ClientID in base header

### ClientID Assignment Rules

- Clients MUST use ClientID = 0 (`0x00 0x00 0x00 0x00`) in initial JOIN messages
- Brokers MUST assign ClientID ≥ 1000 for regular clients
- ClientIDs MUST be unique within broker scope
- ClientID assignment persists for connection lifetime
- Reconnecting clients receive new ClientID assignments

### ClientID Transmission Examples

**JOIN Message (Client → Broker):**

```
Base Header:
[0x01] [0x00] [0x00 0x00 0x00 0x00] [16 zero bytes] [header_len] [payload_len]
 Ver   Type    ClientID=0(UNASSIGNED)   Reserved      Variable     Variable
```

**Response Message (Broker → Client):**

```
Base Header:
[0x01] [0x02] [0x00 0x00 0x03 0xE8] [16 zero bytes] [header_len] [payload_len]
 Ver   Type    ClientID=1000(Assigned)  Reserved      Variable     Variable
```

**Subsequent Messages (Client → Any):**

```
Base Header:
[0x01] [0x01] [0x00 0x00 0x03 0xE8] [16 zero bytes] [header_len] [payload_len]
 Ver   Type    ClientID=1000(Assigned)  Reserved      Variable     Variable
```

### Message Size Calculation

**For JOIN message with token auth:**

- Base header: 34 bytes
- Header (with auth): ~32 bytes (MessagePack encoded)
- Payload length: 8 bytes
- Payload: 0 bytes (empty)
- **Total: ~74 bytes**

**For typical request:**

- Base header: 34 bytes
- Header (routing + reqrep): ~60 bytes
- Payload length: 8 bytes
- Payload: variable (depends on request data)
- **Total: ~102 bytes + payload size**

## Connection Flow

1. **Transport Connection**: Client establishes transport-level connection
2. **Join Handshake**: Client sends JOIN message with ClientID=0 and optional authentication
3. **ClientID Assignment**: Broker validates credentials and assigns unique ClientID (≥1000)
4. **Assignment Response**: Broker responds with assigned ClientID or error status
5. **Keep-alive Setup**: Optional keep-alive mechanism activation via PING/PONG
6. **Message Exchange**: Clients can exchange messages using assigned ClientID
7. **Topic Management**: Clients can subscribe/unsubscribe to topics for pub/sub
8. **Connection Monitoring**: Ongoing keep-alive messages to maintain connection health
9. **Clean Disconnect**: Proper connection termination or timeout handling

## Protocol Rules

### Message Validation

- REQ and REP messages must have exactly one routing entry
- NOTIF messages can have multiple routing entries
- PUB messages with routing field target only specified subscribers, without routing field target all subscribers
- BCAST messages must not have routing entries
- SUB and UNSUB messages must have topic field
- PING and PONG messages must have keepalive field with timestamp
- PONG timestamp must match the corresponding PING timestamp
- Status codes are optional in responses and default to 200 if omitted

### Connection Requirements

- All connections require JOIN handshake for client registration
- ClientID must be unique within broker scope
- Authentication is optional but configurable per broker
- Brokers may reject connections based on authentication, capacity, or policy
- Keep-alive is optional but recommended for long-lived connections
- Connection timeout should be at least 3x the keep-alive interval

### Topic Rules

- Topic names are UTF-8 encoded strings
- Topic names are case-sensitive
- Empty topic names are valid
- Clients receive PUB messages only for subscribed topics
- Unsubscribe removes subscription immediately
- Multiple subscriptions to the same topic are idempotent

## Message Size Limits

- **Maximum message size**: 1GB (1,073,741,824 bytes)
- **Maximum header size**: 64KB (65,536 bytes)
- **Maximum payload size**: 1GB - header size - base header size
- **Minimum message size**: 34 bytes (empty header and payload)
- **Keep-alive message size**: ~60 bytes (base header + keep-alive header)

## Implementation Notes

- All integers use network byte order (big-endian)
- MessagePack is used for structured data encoding (Header and Payload)
- Base header fields use fixed binary encoding for parsing efficiency
- All strings must be UTF-8 encoded
- Implementations should validate message structure according to type-specific rules
- Reserved bytes in base header must be set to zero and ignored on parsing
- Implementations may impose additional limits on message/header sizes for resource management
