//! Core protocol types and data structures

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Protocol version type
pub type ProtocolVersion = u8;

/// Client identifier
pub type ClientId = u32;

/// Message types as defined in the protocol v1.0.0
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
/// Defines the type of a fleximq message. This is part of the fixed header.
pub enum MessageType {
    /// Client request to join the network.
    Join         = 0,
    /// A message sent from a client expecting a response from a peer.
    Request      = 1,
    /// A message sent from a peer in response to a `Request` message.
    Response     = 2,
    /// A one-way message sent from one peer to another, not expecting a
    /// response.
    Notification = 3,
    /// A message sent from a client to all other connected clients (or a subset
    /// based on group/topic).
    Broadcast    = 4,
    /// A message published to a specific topic, to be delivered to subscribed
    /// clients.
    Publish      = 5,
    /// Client request to subscribe to a specific topic.
    Subscribe    = 6,
    /// Client request to unsubscribe from a specific topic.
    Unsubscribe  = 7,
}

impl MessageType {
    /// Convert from u8, returning None for unknown values
    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Join),
            1 => Some(Self::Request),
            2 => Some(Self::Response),
            3 => Some(Self::Notification),
            4 => Some(Self::Broadcast),
            5 => Some(Self::Publish),
            6 => Some(Self::Subscribe),
            7 => Some(Self::Unsubscribe),
            _ => None,
        }
    }

    /// Convert to u8
    #[must_use]
    pub const fn to_u8(self) -> u8 {
        self as u8
    }

    /// Get the string representation for error messages
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Join => "JOIN",
            Self::Request => "REQ",
            Self::Response => "REP",
            Self::Notification => "NOTIF",
            Self::Broadcast => "BCAST",
            Self::Publish => "PUB",
            Self::Subscribe => "SUB",
            Self::Unsubscribe => "UNSUB",
        }
    }
}

/// HTTP-like status codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StatusCode(pub u16);

impl StatusCode {
    /// 202 Accepted
    pub const ACCEPTED: Self = Self(202);
    /// 604 Authentication Failed
    pub const AUTHENTICATION_FAILED: Self = Self(604);
    /// 502 Bad Gateway
    pub const BAD_GATEWAY: Self = Self(502);
    // Client error codes (400-499)
    /// 400 Bad Request
    pub const BAD_REQUEST: Self = Self(400);
    // Protocol specific codes (600-699)
    /// 600 Client Not Found
    pub const CLIENT_NOT_FOUND: Self = Self(600);
    /// 409 Conflict
    pub const CONFLICT: Self = Self(409);
    /// 201 Created
    pub const CREATED: Self = Self(201);
    /// 403 Forbidden
    pub const FORBIDDEN: Self = Self(403);
    /// 504 Gateway Timeout
    pub const GATEWAY_TIMEOUT: Self = Self(504);
    // Server error codes (500-599)
    /// 500 Internal Server Error
    pub const INTERNAL_SERVER_ERROR: Self = Self(500);
    /// 602 Invalid Routing
    pub const INVALID_ROUTING: Self = Self(602);
    /// 605 Join Rejected
    pub const JOIN_REJECTED: Self = Self(605);
    /// 405 Method Not Allowed
    pub const METHOD_NOT_ALLOWED: Self = Self(405);
    /// 404 Not Found
    pub const NOT_FOUND: Self = Self(404);
    /// 501 Not Implemented
    pub const NOT_IMPLEMENTED: Self = Self(501);
    /// 204 No Content
    pub const NO_CONTENT: Self = Self(204);
    // Success codes (200-299)
    /// 200 OK
    pub const OK: Self = Self(200);
    /// 413 Payload Too Large
    pub const PAYLOAD_TOO_LARGE: Self = Self(413);
    /// 408 Request Timeout
    pub const REQUEST_TIMEOUT: Self = Self(408);
    /// 503 Service Unavailable
    pub const SERVICE_UNAVAILABLE: Self = Self(503);
    /// 603 Subscription Failed
    pub const SUBSCRIPTION_FAILED: Self = Self(603);
    /// 429 Too Many Requests
    pub const TOO_MANY_REQUESTS: Self = Self(429);
    /// 601 Topic Not Found
    pub const TOPIC_NOT_FOUND: Self = Self(601);
    /// 401 Unauthorized
    pub const UNAUTHORIZED: Self = Self(401);

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
    /// Client name for service identification
    pub client_name: String,
    /// Routing path
    pub path:        String,
    /// Optional client ID (required for response messages)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id:   Option<ClientId>,
}

/// Request/Response correlation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a request or response message, used in request-reply patterns.
/// This structure is part of the message header and provides context for the
/// request or response.
pub struct RequestResponse {
    #[serde(rename = "type")]
    /// The type of the request or response (e.g., Request, Response, Error).
    pub req_type: RequestResponseType,
    /// A unique identifier for the request, used to correlate requests with
    /// responses.
    pub id:       String,
}

impl RequestResponse {
    #[must_use]
    pub fn req(id: impl Into<String>) -> Self {
        Self {
            req_type: RequestResponseType::Request,
            id: id.into(),
        }
    }

    #[must_use]
    pub fn res(id: impl Into<String>) -> Self {
        Self {
            req_type: RequestResponseType::Correlation,
            id: id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// Represents the type of request-response pattern.
pub enum RequestResponseType {
    /// Indicates a standard request.
    Request,
    /// Indicates a correlation ID for request-response matching.
    Correlation,
}

/// Authentication information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Auth {
    #[serde(rename = "type")]
    /// The type of authentication used.
    pub auth_type:   AuthType,
    #[serde(flatten)]
    /// The credentials for authentication.
    pub credentials: AuthCredentials,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// The type of authentication used.
pub enum AuthType {
    /// Token-based authentication.
    Token,
    /// Basic authentication (username and password).
    Basic,
    /// API key authentication.
    #[serde(rename = "api_key")]
    ApiKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
/// Authentication credentials.
pub enum AuthCredentials {
    /// Token-based authentication credentials.
    Token {
        /// Authentication token.
        token: String,
    },
    /// Basic authentication credentials (username and password).
    Basic {
        /// Username for basic authentication.
        username: String,
        /// Password for basic authentication.
        password: String,
    },
    /// API key authentication credentials.
    ApiKey {
        /// API key for authentication.
        api_key: String,
    },
}

impl Auth {
    /// Creates a new `Auth` instance with token authentication.
    ///
    /// # Arguments
    ///
    /// * `token`: The authentication token.
    pub fn token(token: impl Into<String>) -> Self {
        Self {
            auth_type:   AuthType::Token,
            credentials: AuthCredentials::Token {
                token: token.into(),
            },
        }
    }

    /// Creates a new `Auth` instance with basic authentication.
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            auth_type:   AuthType::Basic,
            credentials: AuthCredentials::Basic {
                username: username.into(),
                password: password.into(),
            },
        }
    }

    /// Create API key authentication
    pub fn api_key(api_key: impl Into<String>) -> Self {
        Self {
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
    /// Routing information for the message header.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing: Option<Vec<Routing>>,

    /// Request/response correlation information.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reqrep: Option<RequestResponse>,

    /// Topic name for publish/subscribe messages.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,

    /// Status code indicating success or error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<StatusCode>,

    /// Authentication information.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<Auth>,

    /// Client name for JOIN messages
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_name: Option<String>,

    /// Additional fields for extensibility
    #[serde(flatten)]
    pub additional: HashMap<String, rmpv::Value>,
}

impl Header {
    /// Create an empty header
    #[must_use]
    pub fn new() -> Self {
        Self {
            routing:     None,
            reqrep:      None,
            topic:       None,
            status:      None,
            auth:        None,
            client_name: None,
            additional:  HashMap::new(),
        }
    }

    /// Add routing information
    #[must_use]
    pub fn with_routing(mut self, routing: Vec<Routing>) -> Self {
        self.routing = Some(routing);
        self
    }

    /// Add single routing target with client name and path
    #[must_use]
    pub fn with_route(mut self, client_name: impl Into<String>, path: impl Into<String>) -> Self {
        self.routing = Some(vec![Routing {
            client_name: client_name.into(),
            path:        path.into(),
            client_id:   None,
        }]);
        self
    }

    /// Add single routing target with client name, path, and client ID
    #[must_use]
    pub fn with_route_and_id(
        mut self,
        client_name: impl Into<String>,
        path: impl Into<String>,
        client_id: ClientId,
    ) -> Self {
        self.routing = Some(vec![Routing {
            client_name: client_name.into(),
            path:        path.into(),
            client_id:   Some(client_id),
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
    pub const fn with_status(mut self, status: StatusCode) -> Self {
        self.status = Some(status);
        self
    }

    /// Add authentication
    #[must_use]
    pub fn with_auth(mut self, auth: Auth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Add client name for JOIN messages
    #[must_use]
    pub fn with_client_name(mut self, client_name: impl Into<String>) -> Self {
        self.client_name = Some(client_name.into());
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
    /// Routing field.
    Routing,
    /// Request/response correlation field.
    Reqrep,
    /// Topic field.
    Topic,
    /// Status field.
    Status,
    /// Authentication field.
    Auth,
    /// Client name field.
    ClientName,
}

impl HeaderField {
    /// Get field name as string
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Routing => "routing",
            Self::Reqrep => "reqrep",
            Self::Topic => "topic",
            Self::Status => "status",
            Self::Auth => "auth",
            Self::ClientName => "client_name",
        }
    }

    /// Check if field is present in header
    #[must_use]
    pub const fn is_present_in(&self, header: &Header) -> bool {
        match self {
            Self::Routing => header.routing.is_some(),
            Self::Reqrep => header.reqrep.is_some(),
            Self::Topic => header.topic.is_some(),
            Self::Status => header.status.is_some(),
            Self::Auth => header.auth.is_some(),
            Self::ClientName => header.client_name.is_some(),
        }
    }
}

/// Base message header (fixed 34 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseHeader {
    /// Protocol version number.
    pub version:        ProtocolVersion,
    /// Message type identifier.
    pub message_type:   MessageType,
    /// Client identifier.
    pub client_id:      ClientId,
    /// Reserved bytes for future use.
    pub reserved:       [u8; 16],
    /// Length of the header in bytes.
    pub header_length:  u32,
    /// Length of the payload in bytes.
    pub payload_length: u64,
}

impl BaseHeader {
    /// Create a new base header
    #[must_use]
    pub const fn new(
        message_type: MessageType,
        client_id: ClientId,
        header_length: u32,
        payload_length: u64,
    ) -> Self {
        Self {
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
