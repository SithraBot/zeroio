# fleximq Protocol V1

> Note: All multi-byte numeric values in this protocol are encoded in network byte order (big-endian).
> All structured data (Header and Payload) uses MessagePack format for serialization.

## 1. Introduction

### 1.1. Protocol Version

The current protocol version is 1. This is indicated by the `Version` field in the Base Format.

### 1.2. Key Design Goals

to provide a versatile, programming language-agnostic, and cross-platform messaging protocol. It supports various communication patterns like request/response, pub/sub, and notifications over multiple transport layers. The protocol is designed with a focus on efficiency and clear semantics, making it suitable for distributed systems, plugin architectures, and optimized for Internet of Things (IoT) applications. Compared to protocols like MQTT, fleximq offers a broader and more complex range of communication modes.

### 1.3. Encoding

- **Numeric Values**: All multi-byte numeric values are encoded in network byte order (big-endian).
- **Structured Data**: Header and Payload sections use MessagePack format for serialization.
- **Strings**: All strings must be UTF-8 encoded.

## 2. Core Message Structure

### 2.1. Base Format

| Field         | Type   | Size (bytes) | Description                                                |
| ------------- | ------ | ------------ | ---------------------------------------------------------- |
| Version       | uint8  | 1            | Protocol version (1)                                       |
| Type          | uint8  | 1            | Message type (see Section 3.1)                             |
| ClientID      | uint32 | 4            | Unique client identifier (32-bit big-endian)               |
| Reserved      | bytes  | 16           | Reserved bytes for future use (must be zero)               |
| HeaderLength  | uint32 | 4            | Length of the header section in bytes (32-bit big-endian)  |
| Header        | bytes  | variable     | Variable length header data (MessagePack encoded)          |
| PayloadLength | uint64 | 8            | Length of the payload section in bytes (64-bit big-endian) |
| Payload       | bytes  | variable     | Variable length payload data (MessagePack encoded)         |

**Total base header size: 34 bytes** (1 byte Version + 1 byte Type + 4 bytes ClientID + 16 bytes Reserved + 4 bytes HeaderLength + 8 bytes PayloadLength).

### 2.2. Header

#### 2.2.1. General Structure

The header is a MessagePack encoded map containing various optional fields that provide metadata for the message. The presence and necessity of these fields depend on the `Type` of the message.

```
Header Map (Conceptual Structure):
  "routing"   => Array of Routing Objects (optional)
  "reqrep"    => Request/Response Object (optional)
  "topic"     => String (optional)
  "status"    => Integer (optional, see Section 5.1 Status Codes)
  "auth"      => Authentication Object (optional, see Section 5.2 Authentication Types)
  "keepalive" => Keep-alive Object (optional, see Section 3.7 Keep-alive)
```

#### 2.2.2. Common Header Fields

##### `routing` (Routing Object)

Specifies the destination(s) for the message.

```
Routing Object:
  "client_id" => 32-bit unsigned integer (Target client identifier)
  "path"      => String (Routing path string for the target endpoint/handler)
```

_Example:_

```json
{
  "client_id": 1234,
  "path": "/api/endpoint"
}
```

- `client_id`: Identifies the specific client this routing information pertains to.
- `path`: An application-defined string that can be used by the recipient client to direct the message to a specific internal handler, resource, or function. Its interpretation is context-dependent and up to the application logic.

##### `reqrep` (Request/Response Object)

Used for correlating requests and responses.

```
Request/Response Object:
  "type" => String ("request" or "correlation")
  "id"   => String (Unique identifier for correlation, recommended: ULID)
```

_Example (Request):_

```json
{
  "type": "request",
  "id": "01H2XQMGPE7ZPXXNKQD9XE8S6R"
}
```

_Example (Response Correlation):_

```json
{
  "type": "correlation",
  "id": "01H2XQMGPE7ZPXXNKQD9XE8S6R"
}
```

- For REQ messages: `type` must be "request".
- For REP messages: `type` must be "correlation", and `id` must match the `id` of the original REQ message.

##### `topic` (String)

Specifies the topic for publish/subscribe messages.
_Example: "news_updates"_

##### `status` (Integer)

Indicates the outcome or status of an operation, typically used in REP messages or error NOTIF messages. Values are based on HTTP-like status codes (see Section 5.1).

##### `auth` (Authentication Object)

Contains credentials for authenticating the client. See Section 5.2 for details on different authentication types.

```
Authentication Object:
  "type"     => String ("token", "basic", or "api_key")
  "token"    => String (for token auth)
  "username" => String (for basic auth)
  "password" => String (for basic auth)
  "api_key"  => String (for API key auth)
```

##### `keepalive` (Keep-alive Object)

Used in PING and PONG messages for connection health monitoring. See Section 3.7.

```
Keep-alive Object:
  "timestamp" => 64-bit unsigned integer (Unix timestamp in milliseconds when the message was created)
  "interval"  => 32-bit unsigned integer (Suggested keep-alive interval in seconds, optional, only in PING)
```

#### 2.2.3. Header Field Requirements per Message Type

| Message Type | Required Fields | Optional Fields | Forbidden Fields                          |
| ------------ | --------------- | --------------- | ----------------------------------------- |
| JOIN         | -               | auth            | routing, reqrep, topic, keepalive, status |
| REQ          | routing, reqrep | status          | topic, auth, keepalive                    |
| REP          | routing, reqrep | status          | topic, auth, keepalive                    |
| NOTIF        | routing         | status          | reqrep, topic, auth, keepalive            |
| BCAST        | -               | status          | routing, reqrep, topic, auth, keepalive   |
| PUB          | topic           | routing, status | reqrep, auth, keepalive                   |
| SUB          | topic           | -               | routing, reqrep, status, auth, keepalive  |
| UNSUB        | topic           | -               | routing, reqrep, status, auth, keepalive  |
| PING         | keepalive       | -               | routing, reqrep, topic, status, auth      |
| PONG         | keepalive       | -               | routing, reqrep, topic, status, auth      |

### 2.3. Payload

#### 2.3.1. General Description

The `Payload` field carries the primary data of the message. It is a variable-length sequence of bytes encoded using MessagePack. The structure and interpretation of the data within the payload are application-defined. The `PayloadLength` field in the Base Format specifies the size of this payload data in bytes. If a message has no primary data to convey, the `PayloadLength` can be zero, and the `Payload` field will be empty.

## 3. Message Types and Communication Patterns

### 3.1. Overview of Message Types

| Type  | Name         | Value | Description                             | Relevant Header Fields (Typical) |
| ----- | ------------ | ----- | --------------------------------------- | -------------------------------- |
| JOIN  | Join         | 0     | Join a group/connect to broker          | `auth` (optional)                |
| REQ   | Request      | 1     | Send a request to single client         | `routing`, `reqrep`              |
| REP   | Response     | 2     | Send a response to a request            | `routing`, `reqrep`, `status`    |
| NOTIF | Notification | 3     | Send a notification to specific clients | `routing`                        |
| BCAST | Broadcast    | 4     | Send a broadcast to all clients         | `status` (optional)              |
| PUB   | Topic        | 5     | Publish a message to a topic            | `topic`, `routing` (optional)    |
| SUB   | Subscribe    | 6     | Subscribe to a topic                    | `topic`                          |
| UNSUB | Unsubscribe  | 7     | Unsubscribe from a topic                | `topic`                          |
| PING  | Ping         | 8     | Connection keep-alive ping              | `keepalive`                      |
| PONG  | Pong         | 9     | Connection keep-alive pong              | `keepalive`                      |

### 3.2. Connection Establishment (JOIN)

- **Purpose**: Used by a client to initiate a connection with a broker and register itself. This is the first message a client must send after establishing a transport-level connection.
- **Header**: May contain an `auth` object if authentication is required by the broker.
- **ClientID in Base Header**: Must be 0 (UNASSIGNED) when sent by the client. The broker responds with an assigned ClientID (see Section 4.1).

_Example (Client sends JOIN with Token Authentication):_

```
Base Header: Version=1, Type=JOIN, ClientID=0, HeaderLength=X, PayloadLength=0
Header: {
  "auth": {
    "type": "token",
    "token": "authentication-token"
  }
}
Payload: (empty)
```

### 3.3. Request/Response (REQ, REP)

- **Purpose & Flow**: Enables a client to send a request to another client (or a service represented by a client) and receive a response.
  1. Client A sends a REQ message to Client B. The `reqrep` header field has `type: "request"` and a unique `id`.
  2. Client B processes the request and sends a REP message back to Client A. The `reqrep` header field has `type: "correlation"` and the same `id` as the REQ message.
- **REQ Header**: - `routing`: Must contain exactly one routing entry specifying the target client and path. - `reqrep`: `type` must be "request", `id` must be a unique identifier. - `status`: Optional, rarely used in REQ.
  _Example (REQ Message):_

```
Base Header: Version=1, Type=REQ, ClientID=ClientA_ID, HeaderLength=X, PayloadLength=Y
Header: {
  "routing": [{ "client_id": TargetClientID_or_BrokerServiceID, "path": "/api/users" }],
  "reqrep": { "type": "request", "id": "unique-request-id" }
}
Payload: { "user_data": "..." } // Example payload
```

- **REP Header**: - `routing`: Must contain exactly one routing entry specifying the original requester. - `reqrep`: `type` must be "correlation", `id` must match the `id` from the corresponding REQ. - `status`: Optional (defaults to 200 OK if omitted), indicates the outcome of the request (see Section 5.1).
  _Example (REP Message):_

```
Base Header: Version=1, Type=REP, ClientID=ClientB_ID, HeaderLength=X, PayloadLength=Z
Header: {
  "routing": [{ "client_id": OriginalRequester_ClientA_ID, "path": "/api/users" }], // Path might be omitted or informational in response
  "reqrep": { "type": "correlation", "id": "unique-request-id" },
  "status": 200
}
Payload: { "result": "..." } // Example payload
```

- **Correlation**: The `id` field in `reqrep` is crucial for matching responses to requests. ULID is recommended for its uniqueness.

### 3.4. Notifications (NOTIF)

- **Purpose**: Allows a client or broker to send a one-way informational message to one or more specific clients. No response is expected.
- **Header**: - `routing`: Contains one or more routing entries, each specifying a target `client_id` and `path`. - `status`: Optional, can be used to indicate the nature or severity of the notification (e.g., an error status).
  _Example (NOTIF Message to multiple clients):_

```
Base Header: Version=1, Type=NOTIF, ClientID=SenderID, HeaderLength=X, PayloadLength=Y
Header: {
  "routing": [
    { "client_id": 1234, "path": "/notifications" },
    { "client_id": 5678, "path": "/alerts" }
  ],
  "status": 600 // Example: Client Not Found if a target was problematic, or general info
}
Payload: { "message": "System maintenance soon" } // Example payload
```

### 3.5. Broadcast (BCAST)

- **Purpose**: Allows a client or broker to send a message to all connected and authorized clients.
- **Header**: - `status`: Optional, can provide context to the broadcast. - No `routing` field is allowed as the message is implicitly for all clients.
  _Note: The broker is responsible for fanning out the BCAST message. Clients typically send BCAST to the broker, which then distributes it._
  _Example (BCAST Message):_

```
Base Header: Version=1, Type=BCAST, ClientID=SenderID, HeaderLength=X, PayloadLength=Y
Header: {
  "status": 200 // Optional, e.g., general announcement
}
Payload: { "announcement": "New feature available!" } // Example payload
```

### 3.6. Publish/Subscribe (PUB, SUB, UNSUB)

- **Purpose & Flow**: Enables a publish-subscribe messaging pattern.
  1. Clients interested in certain types of messages send SUB messages to the broker, specifying a `topic`.
  2. A publisher client sends a PUB message to the broker, specifying a `topic` and the message content.
  3. The broker delivers the PUB message to all clients currently subscribed to that `topic`.
  4. Clients can send UNSUB messages to stop receiving messages for a `topic`.
- **PUB Header**: - `topic`: Required, specifies the topic of the message. - `routing`: Optional. If present, the broker might use it to further filter which subscribers on the topic receive the message (e.g., if subscribers also registered specific paths or if routing is to a specific broker instance that manages the topic). _Clarification: By default, if `routing` is absent, the message goes to all subscribers of the `topic`. If `routing` is present, its interpretation for PUB can be broker-specific, potentially targeting a subset of subscribers or a specific broker handling that topic shard._ - `status`: Optional.
  _Example (PUB Message):_

```
Base Header: Version=1, Type=PUB, ClientID=PublisherID, HeaderLength=X, PayloadLength=Y
Header: {
  "topic": "news_updates",
  // "routing": [{ "client_id": SomeSpecificSubscriberID_or_BrokerServiceID, "path": "/regional" }] // Optional routing
}
Payload: { "article": "New discovery..." } // Example payload
```

- **SUB Header**: - `topic`: Required, specifies the topic to subscribe to.
  _Example (SUB Message):_

```
Base Header: Version=1, Type=SUB, ClientID=SubscriberID, HeaderLength=X, PayloadLength=0
Header: {
  "topic": "news_updates"
}
Payload: (empty)
```

- **UNSUB Header**: - `topic`: Required, specifies the topic to unsubscribe from.
  _Example (UNSUB Message):_

```
Base Header: Version=1, Type=UNSUB, ClientID=SubscriberID, HeaderLength=X, PayloadLength=0
Header: {
  "topic": "news_updates"
}
Payload: (empty)
```

### 3.7. Keep-alive (PING, PONG)

The protocol includes a built-in keep-alive mechanism using PING and PONG messages to maintain connection health and detect disconnections.

- **Purpose**:
  - **PING Messages**: Sent by either party (client or broker) to test connection liveness.
  - **PONG Messages**: Sent in response to PING messages to confirm connection health.
- **Header**: Both PING and PONG messages must contain a `keepalive` object.
  - `keepalive.timestamp`: Unix timestamp in milliseconds when the PING/PONG message was created.
  - `keepalive.interval`: (Optional, PING only) Suggested keep-alive interval in seconds for subsequent keep-alives.

#### 3.7.1. Keep-alive Mechanism and Rules

1.  **PING Initiation**: Either client or broker can initiate keep-alive by sending a PING message.
2.  **PONG Response**: The recipient of a PING message MUST respond with a PONG message. The `keepalive.timestamp` in the PONG message MUST be identical to the `timestamp` from the received PING message.
3.  **Timeout Detection**: The sender of a PING should start a timer. If a corresponding PONG is not received within a reasonable timeout period (e.g., PONG timeout), the sender should consider the connection to be dead or degraded.
4.  **Interval Suggestion**: A PING message MAY include a `keepalive.interval` field to suggest a new interval for future keep-alive checks. The recipient is not obligated to honor this suggestion but may use it to adjust its own keep-alive timers.
5.  **Automatic Keep-alive**: Implementations MAY automatically send PING messages at regular intervals if no other data traffic is flowing, to prevent idle timeouts by intermediaries and to proactively check connection health.

#### 3.7.2. Keep-alive Examples

**PING Message:**

```
Base Header: Version=1, Type=PING, ClientID=MyID, HeaderLength=X, PayloadLength=0
Header: {
  "keepalive": {
    "timestamp": 1703123456789, // Current time in ms
    "interval": 30              // Suggests next ping in 30s
  }
}
Payload: (empty)
```

**PONG Response:**

```
Base Header: Version=1, Type=PONG, ClientID=PeerID, HeaderLength=X, PayloadLength=0
Header: {
  "keepalive": {
    "timestamp": 1703123456789 // Must match the timestamp from the PING
  }
}
Payload: (empty)
```

#### 3.7.3. Keep-alive Best Practices

- **Default Interval**: A common default keep-alive interval is 30 seconds.
- **PONG Timeout**: The timeout for receiving a PONG should typically be a multiple of the keep-alive interval (e.g., 2x to 3x the interval). This accounts for network latency. For an interval of 30s, a PONG timeout of 60-90s might be appropriate.
- **Failed Connections**: Implementations should define a strategy for handling connections where PONGs are consistently missed, such as attempting a limited number of retries before declaring the connection dead, and potentially using exponential backoff for reconnection attempts.
- **Graceful Degradation**: Applications should be designed to handle keep-alive failures gracefully, such as by attempting to reconnect or notifying the user.

## 4. Connection Management

### 4.1. ClientID

#### 4.1.1. Special ClientID Values

The protocol defines specific `ClientID` values for system operations and client identification:

- **0 (UNASSIGNED)**: `0x00000000`. Used by clients in their initial JOIN message before a unique `ClientID` is assigned by the broker.
- **1 (BROKER)**: `0x00000001`. Reserved for messages originating from or explicitly targeting the broker itself for administrative or internal operations. _Clarification: The exact usage of ClientID=1 may vary depending on broker implementation, e.g., for broker-to-broker communication or system messages._
- **1000 - 4294967294** (`0x000003E8` - `0xFFFFFFFE`): Valid range for dynamic `ClientID` assignment by the broker to connected clients.
- **4294967295 (MAX_UINT32)**: `0xFFFFFFFF`. Reserved for future use or special broadcast/multicast scopes if defined.

#### 4.1.2. ClientID Assignment Rules

- Clients MUST use `ClientID = 0` (UNASSIGNED) in the `ClientID` field of the Base Header for their initial JOIN message.
- Brokers MUST assign a unique `ClientID` from the valid dynamic range (1000 to 4294967294, inclusive) to each successfully authenticated and accepted client.
- Assigned `ClientID`s MUST be unique within the scope of a single broker instance.
- The assigned `ClientID` persists for the lifetime of the connection.
- If a client disconnects and then reconnects, it MUST perform the Join Handshake again and will typically receive a new, unique `ClientID`. Brokers SHOULD NOT reuse `ClientID`s immediately to avoid message misdelivery for clients that might be experiencing a temporary disconnection.

### 4.2. Join Handshake Process

The Join Handshake is the mandatory process for a client to register with the broker and obtain a unique `ClientID`.

1.  **Transport Connection**: The client first establishes a transport-level connection (e.g., TCP, WebSocket) to the broker.
2.  **Initial JOIN Message**:
    - The client sends a JOIN message to the broker.
    - The `ClientID` field in the Base Header of this JOIN message MUST be set to 0 (UNASSIGNED).
    - The Header section of the JOIN message MAY include an `auth` object if the client wishes to authenticate or if the broker requires authentication.
3.  **Broker Processing**:
    - The broker receives the JOIN message.
    - If an `auth` object is present, the broker validates the authentication credentials. If authentication fails or is required but not provided, the broker may send back a REP message with an appropriate error `status` (e.g., 401 Unauthorized, 403 Forbidden) and then close the connection, or close it directly.
    - If authentication is successful (or not required), the broker assigns a unique `ClientID` to the client from the valid dynamic range (e.g., ≥1000).
4.  **Broker Response (Assignment or Rejection)**:
    - **Success**: The broker sends a REP message (or a specialized JOIN_ACK type if defined, though REP is generally used here) back to the client.
      - The `ClientID` field in the Base Header of this response message is set to the newly assigned unique `ClientID` for the client.
      - The `Header` of this REP message might contain a `status` code (e.g., 200 OK or 201 Created) and could optionally include other connection parameters or confirmation details if defined by the broker.
      - The `reqrep` field in the header should correlate if the client's JOIN message included a `reqrep.id`.
    - **Failure (Post-Auth)**: If a `ClientID` cannot be assigned (e.g., broker capacity reached), the broker sends a REP message with an appropriate error `status` (e.g., 503 Service Unavailable) and may close the connection.
5.  **Client Confirmation and Usage**:
    - The client receives the broker's response.
    - If the response indicates success and contains an assigned `ClientID`, the client MUST use this assigned `ClientID` in the `ClientID` field of the Base Header for all subsequent messages it sends during the current connection.
    - If the response indicates an error, the client should handle it appropriately (e.g., display error, retry, abort).

#### 4.2.1. ClientID Transmission Examples

**1. JOIN Message (Client → Broker):**
Client initiates connection with `ClientID = 0`.

```
Base Header:
[0x01] [0x00] [0x00 0x00 0x00 0x00] [16 zero bytes] [header_len] [payload_len]
 Ver   Type    ClientID=0(UNASSIGNED)   Reserved      (variable)   (variable)
```

_Header (Example with token auth):_

```json
{
  "auth": { "type": "token", "token": "client-auth-token" }
}
```

_Payload: (typically empty for JOIN)_

**2. Response Message (Broker → Client):**
Broker responds, assigning `ClientID = 1000` (example).

```
Base Header:
[0x01] [0x02] [0x00 0x00 0x03 0xE8] [16 zero bytes] [header_len] [payload_len]
 Ver   Type    ClientID=1000(Assigned)  Reserved      (variable)   (variable)
```

_(Note: Original example used Type=REP (0x02). This is a common way to acknowledge JOIN)_
_Header (Example):_

```json
{
  "status": 200 // OK
  // "reqrep": { "type": "correlation", "id": "if_client_sent_one" } // If client's JOIN had reqrep.id
}
```

_Payload: (typically empty or contains broker info)_

**3. Subsequent Messages (Client → Broker/Other Client):**
Client now uses its assigned `ClientID = 1000`.

```
Base Header:
[0x01] [0x01] [0x00 0x00 0x03 0xE8] [16 zero bytes] [header_len] [payload_len]
 Ver   Type    ClientID=1000(Assigned)  Reserved      (variable)   (variable)
```

_(Example: This could be a REQ message, Type=0x01)_

### 4.3. Connection Flow Summary

1.  **Transport Connection**: Client establishes a transport-level connection with the broker (e.g., TCP, WebSocket).
2.  **Join Handshake**:
    - Client sends JOIN message (ClientID=0, optional `auth` in Header).
    - Broker validates (if `auth` provided), assigns a unique ClientID (e.g., >=1000).
    - Broker sends a response (e.g., REP message) with the assigned ClientID in its Base Header's `ClientID` field and a success `status` in its Header. Or, an error `status` if Join fails.
3.  **Operational Phase**:
    - Client uses the assigned ClientID for all subsequent messages.
    - Clients can exchange REQ/REP, PUB/SUB, NOTIF, BCAST messages as per protocol rules.
    - Optional keep-alive mechanism (PING/PONG) can be used by either client or broker to monitor connection health.
4.  **Disconnection**:
    - Connections can be terminated cleanly (e.g., client sends a final message and closes, or broker initiates disconnect).
    - Connections can be terminated due to errors, timeouts (including keep-alive failures), or network issues.
    - Implementations should handle abrupt disconnections gracefully.

## 5. Data Representation Details

### 5.1. Status Codes

The protocol uses HTTP-like status codes within the optional `status` field in the message Header. These codes provide standardized feedback on the outcome of operations. If a response message (e.g., REP) omits the `status` field, it defaults to `200 OK`.

#### 5.1.1. Success Codes (200-299)

- **200 OK**: Request completed successfully. (Default for REP if `status` is omitted)
- **201 Created**: A resource was created successfully (e.g., after a REQ that results in entity creation).
- **202 Accepted**: Request accepted for processing, but processing is not yet complete (e.g., for asynchronous operations).
- **204 No Content**: Request completed successfully, but there is no data to return in the payload. (PayloadLength would be 0).

#### 5.1.2. Client Error Codes (400-499)

- **400 Bad Request**: The request was malformed, contained invalid syntax, or included invalid parameters in the header or payload.
- **401 Unauthorized**: Authentication is required, and the client either did not provide credentials or the provided credentials are invalid.
- **403 Forbidden**: The authenticated client does not have permission to perform the requested operation or access the requested resource.
- **404 Not Found**: The requested resource (e.g., specified by a `path` in routing or a `topic`) could not be found.
- **405 Method Not Allowed**: The operation (indicated by message Type or path) is not supported for the target resource.
- **408 Request Timeout**: The client did not produce a request within the time that the server was prepared to wait. (Less common in this protocol, usually transport or keep-alive handles timeouts). _Clarification: This status typically refers to client-side send timeout, while 504 is server-side upstream timeout._
- **409 Conflict**: The request could not be completed due to a conflict with the current state of the target resource.
- **413 Payload Too Large**: The request payload is larger than the server is willing or able to process.
- **429 Too Many Requests**: The user has sent too many requests in a given amount of time ("rate limiting").

#### 5.1.3. Server Error Codes (500-599)

- **500 Internal Server Error**: The server encountered an unexpected condition that prevented it from fulfilling the request.
- **501 Not Implemented**: The server does not support the functionality required to fulfill the request (e.g., an unrecognized message Type or feature).
- **502 Bad Gateway**: The server, while acting as a gateway or proxy, received an invalid response from an upstream server it accessed to fulfill the request.
- **503 Service Unavailable**: The server is temporarily unable to handle the request due to overloading or maintenance.
- **504 Gateway Timeout**: The server, while acting as a gateway or proxy, did not receive a timely response from an upstream server.

#### 5.1.4. Protocol Specific Codes (600-699)

These codes are defined by this protocol for conditions not covered by standard HTTP-like codes.

- **600 Client Not Found**: The target `client_id` specified in a `routing` object does not correspond to an active and known client.
- **601 Topic Not Found**: The `topic` specified in a PUB, SUB, or UNSUB message does not exist or is not recognized by the broker.
- **602 Invalid Routing**: The `routing` information provided in the message header is invalid, malformed, or cannot be processed (e.g., missing `client_id` or `path` where expected).
- **603 Subscription Failed**: The broker failed to process a SUB request for reasons other than Topic Not Found (e.g., client exceeded subscription limit, authorization failed for the topic).
- **604 Authentication Failed**: General authentication failure not covered by 401 (e.g. malformed auth object, unsupported auth type).
- **605 Join Rejected**: The JOIN request was rejected by the broker for reasons other than authentication (e.g., broker at max capacity, client IP banned).

### 5.2. Authentication Types

Authentication details are carried within the `auth` object in the message Header, primarily used during the JOIN message.

#### 5.2.1. Token Authentication

- `auth.type`: "token"
- `auth.token`: The bearer token string.
  _Example Header snippet:_
  ```json
  {
    "auth": {
      "type": "token",
      "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
    }
  }
  ```

#### 5.2.2. Basic Authentication

- `auth.type`: "basic"
- `auth.username`: The username string.
- `auth.password`: The password string.
  _Example Header snippet:_
  ```json
  {
    "auth": {
      "type": "basic",
      "username": "user123",
      "password": "securePassword!"
    }
  }
  ```
  _Note: Basic authentication transmits credentials in a way that might be insecure if the transport layer is not encrypted. Use with TLS/WSS is strongly recommended._

#### 5.2.3. API Key Authentication

- `auth.type`: "api_key"
- `auth.api_key`: The API key string.
  _Example Header snippet:_
  ```json
  {
    "auth": {
      "type": "api_key",
      "api_key": "abcdef1234567890zyxwvu"
    }
  }
  ```

## 6. Transport Layer

### 6.1. Supported Transports

The protocol is designed to operate over various transport mechanisms. The choice of transport affects how connections are established and managed at a lower level.

| Transport | URL Scheme | Description                                                    | Connection Initiator | ClientID Assignment |
| :-------- | :--------- | :------------------------------------------------------------- | :------------------- | :------------------ |
| TCP       | `tcp://`   | Standard TCP socket connection.                                | Client               | Broker              |
| IPC       | `ipc://`   | Inter-Process Communication (e.g., Unix Sockets, Named Pipes). | Client               | Broker              |
| WebSocket | `ws://`    | WebSocket connection (typically over HTTP/HTTPS).              | Client               | Broker              |
| STDIO     | `stdio://` | Standard Input/Output pipes for subprocesses.                  | Broker               | Broker              |

- **Connection Initiator**: Specifies which party typically initiates the transport-level connection.
- **ClientID Assignment**: Indicates which party is responsible for assigning the unique `ClientID` during the protocol handshake (always the Broker, except for STDIO where the Broker may pre-assign or use a simplified scheme if it spawns the client). _Clarification for STDIO: The broker assigns the ClientID, but since it spawns the process, the assignment can be part of the spawning mechanism rather than a typical JOIN message response._

### 6.2. Transport URLs

These examples illustrate common URL formats for connecting to a broker.

- **TCP**: `tcp://hostname:port`
  - Example: `tcp://localhost:8080`, `tcp://192.168.1.100:7890`
- **IPC**: `ipc:///path/to/socket` (Unix-like) or `ipc://pipename` (Windows-like)
  - Example (Unix): `ipc:///tmp/fleximq.sock`
  - Example (Windows): `ipc://fleximq_pipe`
  - See Section 6.3 for detailed IPC path handling.
- **WebSocket**: `ws://hostname:port/path` (or `wss://` for secure WebSocket)
  - Example: `ws://localhost:8080/ws`, `wss://example.com/messaging`
- **STDIO**: `stdio://`
  - This scheme indicates that the client will communicate with the broker over its standard input and standard output streams. It's typically used when the broker process spawns the client process. No hostname or port is applicable.

### 6.3. IPC Path Handling

The IPC transport requires platform-specific path processing for Unix domain sockets and Windows named pipes to ensure cross-platform compatibility and ease of use.

#### 6.3.1. Unix Domain Sockets

- **Path Format**: Accepts absolute paths (e.g., `/var/run/app.sock`) or relative paths (e.g., `app.sock`).
- **Default Directory for Relative Paths**: If a relative path is provided (e.g., `socket.sock`), it is typically resolved relative to a default directory like `/tmp` (resulting in `/tmp/socket.sock`). This default can be implementation-dependent.
- **Directory Creation**: Implementations (specifically the listener/broker side) SHOULD automatically create parent directories for the socket file if they do not already exist, but only for the part of the path that needs creation. For example, if creating `/var/run/app/socket.sock` and `/var/run/app` doesn't exist, it should be created. However, it should not attempt to create `/var` or `/var/run` if they don't exist.
- **Path Preservation**: Absolute paths are used as-is, ensuring predictable socket locations.
- **Cleanup**: The socket file SHOULD be removed by the listener when it shuts down cleanly to prevent issues with subsequent listener startups.

#### 6.3.2. Windows Named Pipes

- **Path Format**: Can accept simple pipe names (e.g., `myapp_pipe`) or file-style paths (e.g., `C:\temp\app.pipe`).
- **Internal Pipe Namespace**: All pipe names are internally translated to the Windows named pipe namespace format: `\\.\pipe\{extracted_name}`.
- **Name Extraction from File-Style Paths**: If a file-style path is given (e.g., `C:\temp\app.pipe`), the base name of the file (e.g., `app`) is typically extracted to form the pipe name. File extensions (like `.pipe`) are often stripped.
- **Simplified Naming**: Using simple names (e.g., `ipc://my_pipe_name`) is encouraged for clarity.
- **Instance Management**: Typically, a single pipe instance is created by the listener (broker) to handle incoming client connections. Clients connect to this well-known pipe name.

#### 6.3.3. Cross-Platform URL Examples

- **Unix (Absolute)**: `ipc:///var/run/app/socket.sock` → Listener uses path `/var/run/app/socket.sock`.
- **Unix (Relative)**: `ipc://app.sock` → Listener might use `/tmp/app.sock` (dependent on implementation default).
- **Windows (File-Style Path)**: `ipc://C:\mydata\pipes\app.pipe` → Listener uses `\\.\pipe\app` (or `\\.\pipe\app.pipe` depending on extraction logic).
- **Windows (Simple Name)**: `ipc://myapp_service` → Listener uses `\\.\pipe\myapp_service`.

### 6.4. STDIO Transport Details

The STDIO transport is specifically designed for scenarios where a broker process launches and directly communicates with client subprocesses.

- **Connection Initiator**: The broker is always the connection initiator in the sense that it spawns the client process and establishes the communication channel via its standard pipes.
- **Communication Channel**: The broker writes to the client's `stdin` and reads from the client's `stdout`. Conversely, the client writes to its `stdout` and reads from its `stdin`.
- **Use Case**: Ideal for tightly coupled systems where the broker manages worker processes, specialized handlers, or plugins that run as separate executables.
- **Process Lifecycle**: The lifecycle of the client subprocess is typically managed by the broker. The broker may terminate the subprocess if the communication channel is closed or an error occurs.
- **Security**: The client subprocess inherits security permissions and environment variables from the spawning broker process, unless explicitly modified during the spawn.
- **ClientID Assignment with STDIO**: Since the broker spawns the client, it can assign a `ClientID` through various means:
  1.  **Environment Variable**: Pass the ClientID as an environment variable when spawning the client.
  2.  **Command-line Argument**: Pass the ClientID as a command-line argument to the client process.
  3.  **Initial Message**: The broker could send a first message (outside the typical JOIN) over `stdin` to the client, providing its `ClientID`.
      The standard JOIN message with `ClientID=0` might be bypassed or simplified in STDIO setups if the `ClientID` is pre-assigned. If a standard handshake is still desired, the broker would act as the authority responding to the JOIN.

## 7. Operational Considerations

### 7.1. Message Size Limits

To ensure stability and prevent resource exhaustion, implementations should define and enforce message size limits. The following are suggested limits, but can be configured.

- **Maximum Total Message Size**: 1GB (1,073,741,824 bytes). This is the sum of Base Header, Header, and Payload.
- **Maximum Header Size (`HeaderLength`)**: 64KB (65,536 bytes).
- **Maximum Payload Size (`PayloadLength`)**: Calculated as: Maximum Total Message Size - Base Header Size (34 bytes) - Actual `HeaderLength`. Effectively close to 1GB.
- **Minimum Message Size**: 34 bytes. This corresponds to a message with an empty Header (`HeaderLength` = 0) and an empty Payload (`PayloadLength` = 0).
- **Typical Keep-alive Message Size (PING/PONG)**: Approximately 50-70 bytes, depending on the MessagePack encoding of the `keepalive` object. E.g., Base Header (34 bytes) + HeaderLength (4 bytes) + typical `keepalive` object (e.g., ~15-30 bytes for `{"keepalive":{"timestamp":1234567890123,"interval":30}}`).

### 7.2. Message Size Calculation Examples

**1. JOIN Message with Token Authentication:**

- Base Header Fields (Version, Type, ClientID, Reserved, HeaderLength, PayloadLength): 34 bytes (fixed part, excluding variable Header and Payload themselves).
- Header Data (MessagePack encoded `auth` object):
  - `auth` object: `{"auth":{"type":"token","token":"authentication-token-string"}}`
  - Example MessagePack size for a short token: ~40-80 bytes. This is variable. Let's assume **~60 bytes** for this example.
  - So, `HeaderLength` field in Base Header = 60.
- Payload Data: Typically empty for JOIN.
  - `PayloadLength` field in Base Header = 0.
- **Total Estimated Message Size**: 34 bytes (Base) + 60 bytes (Header) + 0 bytes (Payload) = **~94 bytes**.

**2. Typical REQ/REP Message (e.g., API call):**

- Base Header Fields: 34 bytes.
- Header Data (MessagePack encoded `routing` and `reqrep` objects):
  - `routing` object: `{"routing":[{"client_id":1234,"path":"/api/users/get"}]}`
  - `reqrep` object: `{"reqrep":{"type":"request","id":"01H2XQMGPE7ZPXXNKQD9XE8S6R"}}`
  - Example MessagePack size for these combined: ~80-120 bytes. Let's assume **~100 bytes** for this example.
  - So, `HeaderLength` field in Base Header = 100.
- Payload Data: Variable, depends on the actual request/response data.
  - Let `PayloadSize` be the size of the MessagePack encoded payload.
  - `PayloadLength` field in Base Header = `PayloadSize`.
- **Total Estimated Message Size**: 34 bytes (Base) + 100 bytes (Header) + `PayloadSize` = **~134 bytes + PayloadSize**.

### 7.3. General Protocol Rules

#### 7.3.1. Message Validation Rules

Implementations (brokers and clients) MUST validate messages according to these rules. Messages failing validation SHOULD be rejected, possibly with a REP message containing a `400 Bad Request` or a more specific error status.

- **REQ and REP Messages**: Must have exactly one entry in the `routing` array in their Header.
- **NOTIF Messages**: Can have one or more entries in the `routing` array. If `routing` is empty or not present, it's a protocol violation for NOTIF.
- **PUB Messages**:
  - Must have a `topic` field in the Header.
  - The `routing` field in the Header is optional.
    - If `routing` is not present, the message is typically published to all subscribers of the `topic`.
    - If `routing` is present, its interpretation can be broker-specific (e.g., targeting a subset of subscribers, a specific broker instance, or path-based filtering within the topic). Implementations must clearly define this behavior.
- **BCAST Messages**: Must NOT have a `routing` field in the Header.
- **SUB and UNSUB Messages**: Must have a `topic` field in the Header. They must not contain `routing`, `reqrep`, `status`, `auth`, or `keepalive` fields.
- **PING and PONG Messages**: Must have a `keepalive` field in the Header, which must contain a `timestamp`. They must not contain `routing`, `reqrep`, `topic`, or `auth` fields. `status` is also not applicable.
- **PONG `timestamp`**: The `timestamp` in a PONG message's `keepalive` object MUST match the `timestamp` from the corresponding PING message that triggered it.
- **`status` Codes**: Generally optional in response messages (like REP). If omitted in a REP message, it implies `200 OK`. For other messages where `status` is optional (e.g., NOTIF, BCAST, PUB), its absence means no specific status is being conveyed beyond the message's intrinsic purpose.
- **Reserved Bytes**: The 16 `Reserved` bytes in the Base Header MUST be set to zero by the sender and ignored by the receiver upon parsing. This allows for future protocol extensions without breaking backward compatibility for this field.

#### 7.3.2. Connection Lifecycle and Requirements

- **JOIN Handshake**: All client connections (except potentially STDIO as noted) MUST begin with the Join Handshake process (Section 4.2) for client registration and `ClientID` assignment.
- **ClientID Uniqueness**: Assigned `ClientID`s MUST be unique within the operational scope of a broker.
- **Authentication**: While the protocol defines an `auth` header field, the enforcement of authentication is determined by the broker's configuration. Brokers may allow anonymous access or require specific authentication types.
- **Broker Rejection**: Brokers MAY reject JOIN attempts or subsequent messages based on various criteria: authentication failure, invalid message format, resource limits (e.g., maximum connections), or other policy decisions (e.g., IP blacklisting).
- **Keep-alive**: While PING/PONG messages are defined, their use for active keep-alive is optional but highly recommended for long-lived connections or connections traversing networks with idle timeouts. Both clients and brokers can initiate PINGs.
- **Connection Timeout**: If keep-alive is used, the timeout for expecting a PONG response should be configurable and generally be at least 2-3 times the PING interval to accommodate network latency.

#### 7.3.3. Topic and Subscription Rules

- **Topic Names**: Are UTF-8 encoded strings. The definition of valid characters and maximum length for topic names can be implementation-specific, but they should be practical.
- **Case Sensitivity**: Topic names ARE case-sensitive by default (e.g., "MyTopic" is different from "mytopic"). Implementations may offer case-insensitive matching as a configurable option but should clearly document this behavior.
- **Empty Topic Names**: The protocol itself does not forbid empty strings as topic names. However, brokers MAY reject empty topic names as a policy. Generally, meaningful, non-empty topic names are recommended.
- **Subscription Scope**: Clients receive PUB messages only for topics to which they are actively and successfully subscribed.
- **Unsubscription**: An UNSUB message should lead to the immediate removal of the client's subscription for the specified topic. The client should not receive further PUB messages for that topic after the broker processes the UNSUB.
- **Idempotency of Subscriptions**: Multiple SUB messages for the same topic from the same client are typically idempotent. The client remains subscribed; sending SUB again usually has no additional effect beyond potentially refreshing a subscription timer if the broker implements such.
- **Wildcard Subscriptions**: The current protocol specification does not explicitly define wildcard syntax for topic subscriptions (e.g., `news.*` or `updates/#`). If an implementation supports wildcard topics, it must clearly define the syntax and matching semantics.

### 7.4. Implementation Notes

- **Integer Encoding**: All multi-byte integers (e.g., `ClientID`, `HeaderLength`, `PayloadLength`, timestamps, intervals) MUST be encoded in network byte order (big-endian).
- **Structured Data Serialization**: The `Header` (if present and `HeaderLength` > 0) and `Payload` (if present and `PayloadLength` > 0) sections MUST be serialized using MessagePack.
- **Base Header Parsing**: The Base Header fields (Version, Type, ClientID, Reserved, HeaderLength, PayloadLength) use fixed binary encoding and positions, allowing for efficient initial parsing before potentially deserializing the MessagePack Header.
- **String Encoding**: All strings within the protocol (e.g., in Header fields like `path`, `topic`, `id`, `token`, `username`, `password`, `api_key`, and any strings within the MessagePack-encoded Payload) MUST be UTF-8 encoded.
- **Message Structure Validation**: Implementations (both client and broker) MUST rigorously validate incoming messages against the structure defined by their `Type` and the rules in Section 7.3.1. This includes checking for required header fields, forbidden fields, and data types.
- **Resource Management**: Implementations MAY impose stricter limits on message sizes, header sizes, number of routing entries, or other parameters than those suggested in Section 7.1, for effective resource management and to prevent abuse. Such limits should be configurable and clearly documented.
- **Error Handling**: Robust error handling is crucial. Implementations should use appropriate `status` codes in REP messages for errors and handle unexpected data or disconnections gracefully.
- **Extensibility**: The `Reserved` field in the Base Header is intended for future protocol evolution. The use of MessagePack for Header and Payload also offers some flexibility for adding new optional fields without breaking older clients that ignore unknown fields.
