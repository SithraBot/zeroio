//! Core protocol types and data structures

use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

/// Protocol version type
pub type ProtocolVersion = u8;

/// Client identifier
pub type ClientId = u32;

/// Message types as defined in the protocol v1.0.0
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageType {
    Join         = 0,
    Request      = 1,
    Response     = 2,
    Notification = 3,
    Broadcast    = 4,
    Publish      = 5,
    Subscribe    = 6,
    Unsubscribe  = 7,
    Ping         = 8,
    Pong         = 9,
}

impl MessageType {
    /// Convert from u8, returning None for unknown values
    #[must_use]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(MessageType::Join),
            1 => Some(MessageType::Request),
            2 => Some(MessageType::Response),
            3 => Some(MessageType::Notification),
            4 => Some(MessageType::Broadcast),
            5 => Some(MessageType::Publish),
            6 => Some(MessageType::Subscribe),
            7 => Some(MessageType::Unsubscribe),
            8 => Some(MessageType::Ping),
            9 => Some(MessageType::Pong),
            _ => None,
        }
    }

    /// Convert to u8
    #[must_use]
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// Get the string representation for error messages
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            MessageType::Join => "JOIN",
            MessageType::Request => "REQ",
            MessageType::Response => "REP",
            MessageType::Notification => "NOTIF",
            MessageType::Broadcast => "BCAST",
            MessageType::Publish => "PUB",
            MessageType::Subscribe => "SUB",
            MessageType::Unsubscribe => "UNSUB",
            MessageType::Ping => "PING",
            MessageType::Pong => "PONG",
        }
    }
}

/// HTTP-like status codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StatusCode(pub u16);

impl StatusCode {
    pub const ACCEPTED: StatusCode = StatusCode(202);
    pub const AUTHENTICATION_FAILED: StatusCode = StatusCode(604);
    pub const BAD_GATEWAY: StatusCode = StatusCode(502);
    // Client error codes (400-499)
    pub const BAD_REQUEST: StatusCode = StatusCode(400);
    // Protocol specific codes (600-699)
    pub const CLIENT_NOT_FOUND: StatusCode = StatusCode(600);
    pub const CONFLICT: StatusCode = StatusCode(409);
    pub const CREATED: StatusCode = StatusCode(201);
    pub const FORBIDDEN: StatusCode = StatusCode(403);
    pub const GATEWAY_TIMEOUT: StatusCode = StatusCode(504);
    // Server error codes (500-599)
    pub const INTERNAL_SERVER_ERROR: StatusCode = StatusCode(500);
    pub const INVALID_ROUTING: StatusCode = StatusCode(602);
    pub const JOIN_REJECTED: StatusCode = StatusCode(605);
    pub const METHOD_NOT_ALLOWED: StatusCode = StatusCode(405);
    pub const NOT_FOUND: StatusCode = StatusCode(404);
    pub const NOT_IMPLEMENTED: StatusCode = StatusCode(501);
    pub const NO_CONTENT: StatusCode = StatusCode(204);
    // Success codes (200-299)
    pub const OK: StatusCode = StatusCode(200);
    pub const PAYLOAD_TOO_LARGE: StatusCode = StatusCode(413);
    pub const REQUEST_TIMEOUT: StatusCode = StatusCode(408);
    pub const SERVICE_UNAVAILABLE: StatusCode = StatusCode(503);
    pub const SUBSCRIPTION_FAILED: StatusCode = StatusCode(603);
    pub const TOO_MANY_REQUESTS: StatusCode = StatusCode(429);
    pub const TOPIC_NOT_FOUND: StatusCode = StatusCode(601);
    pub const UNAUTHORIZED: StatusCode = StatusCode(401);

    /// Check if this is a success status (200-299)
    #[must_use]
    pub fn is_success(self) -> bool {
        (200..300).contains(&self.0)
    }

    /// Check if this is a client error (400-499)
    #[must_use]
    pub fn is_client_error(self) -> bool {
        (400..500).contains(&self.0)
    }

    /// Check if this is a server error (500-599)
    #[must_use]
    pub fn is_server_error(self) -> bool {
        (500..600).contains(&self.0)
    }

    /// Check if this is a protocol specific error (600-699)
    #[must_use]
    pub fn is_protocol_error(self) -> bool {
        (600..700).contains(&self.0)
    }
}

/// Routing information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Routing {
    pub client_id: ClientId,
    pub path:      String,
}

/// Request/Response correlation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestResponse {
    #[serde(rename = "type")]
    pub req_type: RequestResponseType,
    pub id:       String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestResponseType {
    Request,
    Correlation,
}

/// Keep-alive information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeepAlive {
    pub timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval:  Option<u32>,
}

impl KeepAlive {
    /// Create a new keep-alive with current timestamp
    #[must_use]
    pub fn new() -> Self {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;

        KeepAlive {
            timestamp,
            interval: None,
        }
    }

    /// Create a new keep-alive with current timestamp and interval
    #[must_use]
    pub fn with_interval(interval_seconds: u32) -> Self {
        let mut ka = Self::new();
        ka.interval = Some(interval_seconds);
        ka
    }
}

impl Default for KeepAlive {
    fn default() -> Self {
        Self::new()
    }
}

/// Authentication information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Auth {
    #[serde(rename = "type")]
    pub auth_type:   AuthType,
    #[serde(flatten)]
    pub credentials: AuthCredentials,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    Token,
    Basic,
    #[serde(rename = "api_key")]
    ApiKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AuthCredentials {
    Token { token: String },
    Basic { username: String, password: String },
    ApiKey { api_key: String },
}

impl Auth {
    /// Create token authentication
    pub fn token(token: impl Into<String>) -> Self {
        Auth {
            auth_type:   AuthType::Token,
            credentials: AuthCredentials::Token {
                token: token.into(),
            },
        }
    }

    /// Create basic authentication
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Auth {
            auth_type:   AuthType::Basic,
            credentials: AuthCredentials::Basic {
                username: username.into(),
                password: password.into(),
            },
        }
    }

    /// Create API key authentication
    pub fn api_key(api_key: impl Into<String>) -> Self {
        Auth {
            auth_type:   AuthType::ApiKey,
            credentials: AuthCredentials::ApiKey {
                api_key: api_key.into(),
            },
        }
    }
}

/// Message header fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Header {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing: Option<Vec<Routing>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reqrep: Option<RequestResponse>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<StatusCode>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<Auth>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub keepalive: Option<KeepAlive>,

    /// Additional fields for extensibility
    #[serde(flatten)]
    pub additional: HashMap<String, rmpv::Value>,
}

impl Header {
    /// Create an empty header
    #[must_use]
    pub fn new() -> Self {
        Header {
            routing:    None,
            reqrep:     None,
            topic:      None,
            status:     None,
            auth:       None,
            keepalive:  None,
            additional: HashMap::new(),
        }
    }

    /// Add routing information
    #[must_use]
    pub fn with_routing(mut self, routing: Vec<Routing>) -> Self {
        self.routing = Some(routing);
        self
    }

    /// Add single routing target
    #[must_use]
    pub fn with_route(mut self, client_id: ClientId, path: impl Into<String>) -> Self {
        self.routing = Some(vec![Routing {
            client_id,
            path: path.into(),
        }]);
        self
    }

    /// Add request/response correlation
    #[must_use]
    pub fn with_reqrep(mut self, reqrep: RequestResponse) -> Self {
        self.reqrep = Some(reqrep);
        self
    }

    /// Add topic
    #[must_use]
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Add status code
    #[must_use]
    pub fn with_status(mut self, status: StatusCode) -> Self {
        self.status = Some(status);
        self
    }

    /// Add authentication
    #[must_use]
    pub fn with_auth(mut self, auth: Auth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Add keep-alive information
    #[must_use]
    pub fn with_keepalive(mut self, keepalive: KeepAlive) -> Self {
        self.keepalive = Some(keepalive);
        self
    }
}

impl Default for Header {
    fn default() -> Self {
        Self::new()
    }
}

/// Header field enumeration for validation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderField {
    Routing,
    Reqrep,
    Topic,
    Status,
    Auth,
    Keepalive,
}

impl HeaderField {
    /// Get field name as string
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            HeaderField::Routing => "routing",
            HeaderField::Reqrep => "reqrep",
            HeaderField::Topic => "topic",
            HeaderField::Status => "status",
            HeaderField::Auth => "auth",
            HeaderField::Keepalive => "keepalive",
        }
    }

    /// Check if field is present in header
    #[must_use]
    pub fn is_present_in(&self, header: &Header) -> bool {
        match self {
            HeaderField::Routing => header.routing.is_some(),
            HeaderField::Reqrep => header.reqrep.is_some(),
            HeaderField::Topic => header.topic.is_some(),
            HeaderField::Status => header.status.is_some(),
            HeaderField::Auth => header.auth.is_some(),
            HeaderField::Keepalive => header.keepalive.is_some(),
        }
    }
}

/// Base message header (fixed 34 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseHeader {
    pub version:        ProtocolVersion,
    pub message_type:   MessageType,
    pub client_id:      ClientId,
    pub reserved:       [u8; 16],
    pub header_length:  u32,
    pub payload_length: u64,
}

impl BaseHeader {
    /// Create a new base header
    #[must_use]
    pub fn new(
        message_type: MessageType,
        client_id: ClientId,
        header_length: u32,
        payload_length: u64,
    ) -> Self {
        BaseHeader {
            version: crate::constants::PROTOCOL_VERSION,
            message_type,
            client_id,
            reserved: [0; 16],
            header_length,
            payload_length,
        }
    }

    /// Get total message size including base header
    #[must_use]
    pub fn total_message_size(&self) -> u64 {
        crate::constants::BASE_HEADER_SIZE as u64
            + u64::from(self.header_length)
            + self.payload_length
    }

    /// Check if message size is within limits
    #[must_use]
    pub fn is_size_valid(&self, max_message_size: usize, max_header_size: usize) -> bool {
        self.total_message_size() <= max_message_size as u64
            && self.header_length <= max_header_size as u32
    }
}
