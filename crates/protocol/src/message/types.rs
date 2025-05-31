use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageType {
    Join         = 0, // join a group
    Request      = 1, // send a request to single client
    Response     = 2, // send a response
    Notification = 3, // send a notification to specific clients
    Broadcast    = 4, // send a broadcast to all clients
    Topic        = 5, // send a topic to specific clients
    Subscribe    = 6, // subscribe to a topic
    Unsubscribe  = 7, // unsubscribe from a topic
    Ping         = 8, // connection keep-alive ping
    Pong         = 9, // connection keep-alive pong
}

impl TryFrom<u8> for MessageType {
    type Error = MessageDeserializeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageType::Join),
            1 => Ok(MessageType::Request),
            2 => Ok(MessageType::Response),
            3 => Ok(MessageType::Notification),
            4 => Ok(MessageType::Broadcast),
            5 => Ok(MessageType::Topic),
            6 => Ok(MessageType::Subscribe),
            7 => Ok(MessageType::Unsubscribe),
            8 => Ok(MessageType::Ping),
            9 => Ok(MessageType::Pong),
            _ => Err(MessageDeserializeError::InvalidMessageType),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Routing {
    pub client_id: u32,
    pub path:      String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Reqrep {
    #[serde(rename = "request")]
    Request { id: String },
    #[serde(rename = "correlation")]
    Correlation { id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u16)]
pub enum StatusCode {
    // Success codes (200-299)
    Ok                  = 200,
    Created             = 201,
    Accepted            = 202,

    // Client error codes (400-499)
    BadRequest          = 400,
    Unauthorized        = 401,
    Forbidden           = 403,
    NotFound            = 404,
    MethodNotAllowed    = 405,
    RequestTimeout      = 408,

    // Server error codes (500-599)
    InternalServerError = 500,
    NotImplemented      = 501,
    BadGateway          = 502,
    ServiceUnavailable  = 503,
    GatewayTimeout      = 504,

    // Protocol specific codes (600-699)
    ClientNotFound      = 600,
    TopicNotFound       = 601,
    InvalidRouting      = 602,
    SubscriptionFailed  = 603,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Auth {
    #[serde(rename = "token")]
    Token { token: String },
    #[serde(rename = "basic")]
    Basic { username: String, password: String },
    #[serde(rename = "api_key")]
    ApiKey { api_key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Keepalive {
    pub timestamp: u64,
    pub interval:  Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Header {
    pub routing:   Option<Vec<Routing>>,
    pub reqrep:    Option<Reqrep>,
    pub topic:     Option<String>,
    pub status:    Option<StatusCode>,
    pub auth:      Option<Auth>,
    pub keepalive: Option<Keepalive>,
}

#[derive(Debug, thiserror::Error)]
pub enum MessageDeserializeError {
    #[error("Invalid protocol version")]
    InvalidVersion,
    #[error("Invalid message type")]
    InvalidMessageType,
    #[error("Invalid header data: {0}")]
    InvalidHeaderData(String),
    #[error("Incomplete header")]
    IncompleteHeader,
    #[error("Incomplete payload")]
    IncompletePayload,
    #[error("Invalid routing data: {0}")]
    InvalidRoutingData(String),
    #[error("Deserialization error: {0}")]
    DeserializeError(String),
}

#[derive(Debug, thiserror::Error)]
pub enum MessageEncodeError {
    #[error("Invalid message configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Header serialization error: {0}")]
    HeaderSerializeError(String),
    #[error("Payload serialization error: {0}")]
    PayloadSerializeError(String),
    #[error("Message too large: {0} bytes")]
    MessageTooLarge(usize),
    #[error("Invalid routing data: {0}")]
    InvalidRoutingData(String),
    #[error("Missing required header data: {0}")]
    MissingHeaderData(String),
}

// Special ClientID values
pub const CLIENT_ID_UNASSIGNED: u32 = 0;
pub const CLIENT_ID_BROKER: u32 = 1;
pub const CLIENT_ID_MIN_ASSIGNED: u32 = 1000;
pub const CLIENT_ID_RESERVED: u32 = 4_294_967_295;

// Default keep-alive values
pub const DEFAULT_KEEPALIVE_INTERVAL: u32 = 30; // seconds
pub const DEFAULT_KEEPALIVE_TIMEOUT_MULTIPLIER: u32 = 3;
