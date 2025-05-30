# zeroio Protocol V1

> Note: All multi-byte numeric values in this protocol are encoded in network byte order (big-endian).
> All structured data (Header and Payload) uses MessagePack format for serialization/deserialization.

## Base Format

| Field         | Type       | Description                                                       |
| ------------- | ---------- | ----------------------------------------------------------------- |
| Version       | `u8`       | Protocol version (1)                                              |
| Type          | `u8`       | Message type (Request=0, Response=1, Notification=2, Broadcast=3) |
| ClientID      | `u32`      | Unique client identifier                                          |
| Reserved      | `[u8; 16]` | Reserved bytes for future use                                     |
| HeaderLength  | `u32`      | Length of the header section in bytes                             |
| Header        | `[u8]`     | Variable length header data                                       |
| PayloadLength | `u64`      | Length of the payload section in bytes                            |
| Payload       | `[u8]`     | Variable length payload data                                      |

## Types

```rust
enum MessageType {
    Request      = 0,
    Response     = 1,
    Notification = 2,
    Broadcast    = 3,
}

struct Header {
    // if MessageType is not Broadcast, routing is required
    routing: Option<Vec<Routing>>,
    // if MessageType is Request or Response, request/response is required
    reqrep: Option<Reqrep>,
    // ...other fields
}

// (client_id: u32, path: String)
struct Routing(u32, String);

enum Reqrep {
    Request(String),
    Correlation(String),
}
```

## Header Example

### For a Request to specific clients:

```json
{
    "routing": [[1234, "/path/to/endpoint"], [5678, "/other/path"]],
    "reqrep": {"request": "01H2XQMGPE7ZPXXNKQD9XE8S6R"}
}
```

### For a Broadcast:

```json
{}
```
