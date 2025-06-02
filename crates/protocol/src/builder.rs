//! Fluent builder API for constructing protocol messages

use crate::{
    errors::ProtocolResult,
    message::RawMessage,
    types::{
        Auth, ClientId, Header, KeepAlive, MessageType, RequestResponse, RequestResponseType,
        Routing, StatusCode,
    },
};

/// Fluent builder for protocol messages
///
/// This provides a type-safe, ergonomic API for constructing
/// messages with proper validation and defaults.
#[derive(Debug, Clone)]
pub struct MessageBuilder {
    message_type: MessageType,
    client_id:    ClientId,
    header:       Header,
}

impl MessageBuilder {
    /// Create a new message builder
    #[must_use]
    pub fn new(message_type: MessageType, client_id: ClientId) -> Self {
        MessageBuilder {
            message_type,
            client_id,
            header: Header::new(),
        }
    }

    /// Set routing information
    #[must_use]
    pub fn routing(mut self, routing: Vec<Routing>) -> Self {
        self.header.routing = Some(routing);
        self
    }

    /// Set a single routing target
    #[must_use]
    pub fn route_to(mut self, client_id: ClientId, path: impl Into<String>) -> Self {
        let routing = vec![Routing {
            client_id,
            path: path.into(),
        }];
        self.header.routing = Some(routing);
        self
    }

    /// Set multiple routing targets
    #[must_use]
    pub fn route_to_many<I, S>(mut self, targets: I) -> Self
    where
        I: IntoIterator<Item = (ClientId, S)>,
        S: Into<String>,
    {
        let routing = targets
            .into_iter()
            .map(|(client_id, path)| Routing {
                client_id,
                path: path.into(),
            })
            .collect();
        self.header.routing = Some(routing);
        self
    }

    /// Set request/response correlation
    #[must_use]
    pub fn reqrep(mut self, reqrep: RequestResponse) -> Self {
        self.header.reqrep = Some(reqrep);
        self
    }

    /// Mark as a request with unique ID
    #[must_use]
    pub fn as_request(mut self, id: impl Into<String>) -> Self {
        self.header.reqrep = Some(RequestResponse {
            req_type: RequestResponseType::Request,
            id:       id.into(),
        });
        self
    }

    /// Mark as a response correlating to a request
    #[must_use]
    pub fn as_response(mut self, request_id: impl Into<String>) -> Self {
        self.header.reqrep = Some(RequestResponse {
            req_type: RequestResponseType::Correlation,
            id:       request_id.into(),
        });
        self
    }

    /// Set topic for pub/sub messages
    #[must_use]
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.header.topic = Some(topic.into());
        self
    }

    /// Set status code
    #[must_use]
    pub fn status(mut self, status: StatusCode) -> Self {
        self.header.status = Some(status);
        self
    }

    /// Set authentication
    #[must_use]
    pub fn auth(mut self, auth: Auth) -> Self {
        self.header.auth = Some(auth);
        self
    }

    /// Set token authentication
    #[must_use]
    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.header.auth = Some(Auth::token(token));
        self
    }

    /// Set basic authentication
    #[must_use]
    pub fn auth_basic(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.header.auth = Some(Auth::basic(username, password));
        self
    }

    /// Set API key authentication
    #[must_use]
    pub fn auth_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.header.auth = Some(Auth::api_key(api_key));
        self
    }

    /// Set keep-alive information
    #[must_use]
    pub fn keepalive(mut self, keepalive: KeepAlive) -> Self {
        self.header.keepalive = Some(keepalive);
        self
    }

    /// Set keep-alive with current timestamp
    #[must_use]
    pub fn ping(mut self) -> Self {
        self.header.keepalive = Some(KeepAlive::new());
        self
    }

    /// Set keep-alive with current timestamp and interval
    #[must_use]
    pub fn ping_with_interval(mut self, interval_seconds: u32) -> Self {
        self.header.keepalive = Some(KeepAlive::with_interval(interval_seconds));
        self
    }

    /// Set keep-alive for PONG response
    #[must_use]
    pub fn pong(mut self, ping_timestamp: u64) -> Self {
        self.header.keepalive = Some(KeepAlive {
            timestamp: ping_timestamp,
            interval:  None,
        });
        self
    }

    /// Build the message without payload
    ///
    /// # Errors
    ///
    /// Returns an error if message validation fails or if `RawMessage` creation
    /// fails.
    pub fn build(self) -> ProtocolResult<RawMessage> {
        // Validate message structure
        crate::parser::validate_message_structure(self.message_type, &self.header)?;

        RawMessage::new(self.message_type, self.client_id, self.header, None::<&()>)
    }

    /// Build the message with payload
    ///
    /// # Errors
    ///
    /// Returns an error if message validation fails or if `RawMessage` creation
    /// fails.
    pub fn build_with_payload<T>(self, payload: &T) -> ProtocolResult<RawMessage>
    where
        T: serde::Serialize,
    {
        // Validate message structure
        crate::parser::validate_message_structure(self.message_type, &self.header)?;

        RawMessage::new(
            self.message_type,
            self.client_id,
            self.header,
            Some(payload),
        )
    }
}

/// Specialized builders for different message types
impl MessageBuilder {
    /// Create a JOIN message builder
    #[must_use]
    pub fn join(client_id: ClientId) -> Self {
        Self::new(MessageType::Join, client_id)
    }

    /// Create a REQUEST message builder
    #[must_use]
    pub fn request(client_id: ClientId) -> Self {
        Self::new(MessageType::Request, client_id)
    }

    /// Create a RESPONSE message builder
    #[must_use]
    pub fn response(client_id: ClientId) -> Self {
        Self::new(MessageType::Response, client_id)
    }

    /// Create a NOTIFICATION message builder
    #[must_use]
    pub fn notification(client_id: ClientId) -> Self {
        Self::new(MessageType::Notification, client_id)
    }

    /// Create a BROADCAST message builder
    #[must_use]
    pub fn broadcast(client_id: ClientId) -> Self {
        Self::new(MessageType::Broadcast, client_id)
    }

    /// Create a PUBLISH message builder
    #[must_use]
    pub fn publish(client_id: ClientId) -> Self {
        Self::new(MessageType::Publish, client_id)
    }

    /// Create a SUBSCRIBE message builder
    #[must_use]
    pub fn subscribe(client_id: ClientId) -> Self {
        Self::new(MessageType::Subscribe, client_id)
    }

    /// Create a UNSUBSCRIBE message builder
    #[must_use]
    pub fn unsubscribe(client_id: ClientId) -> Self {
        Self::new(MessageType::Unsubscribe, client_id)
    }

    /// Create a PING message builder
    #[must_use]
    pub fn ping_message(client_id: ClientId) -> Self {
        Self::new(MessageType::Ping, client_id).ping()
    }

    /// Create a PONG message builder
    #[must_use]
    pub fn pong_message(client_id: ClientId, ping_timestamp: u64) -> Self {
        Self::new(MessageType::Pong, client_id).pong(ping_timestamp)
    }
}

/// Convenience methods for common message patterns
impl MessageBuilder {
    /// Create a simple request message
    pub fn simple_request(
        client_id: ClientId,
        target_client_id: ClientId,
        path: impl Into<String>,
        request_id: impl Into<String>,
    ) -> Self {
        Self::request(client_id).route_to(target_client_id, path).as_request(request_id)
    }

    /// Create a simple response message
    pub fn simple_response(
        client_id: ClientId,
        target_client_id: ClientId,
        path: impl Into<String>,
        request_id: impl Into<String>,
        status: StatusCode,
    ) -> Self {
        Self::response(client_id)
            .route_to(target_client_id, path)
            .as_response(request_id)
            .status(status)
    }

    /// Create a simple notification message
    pub fn simple_notification(
        client_id: ClientId,
        target_client_id: ClientId,
        path: impl Into<String>,
    ) -> Self {
        Self::notification(client_id).route_to(target_client_id, path)
    }

    /// Create a simple publish message
    pub fn simple_publish(client_id: ClientId, topic: impl Into<String>) -> Self {
        Self::publish(client_id).topic(topic)
    }

    /// Create a simple subscribe message
    pub fn simple_subscribe(client_id: ClientId, topic: impl Into<String>) -> Self {
        Self::subscribe(client_id).topic(topic)
    }

    /// Create a simple unsubscribe message
    pub fn simple_unsubscribe(client_id: ClientId, topic: impl Into<String>) -> Self {
        Self::unsubscribe(client_id).topic(topic)
    }

    /// Create a JOIN message with token authentication
    pub fn join_with_token(client_id: ClientId, token: impl Into<String>) -> Self {
        Self::join(client_id).auth_token(token)
    }

    /// Create a JOIN message with basic authentication
    pub fn join_with_basic_auth(
        client_id: ClientId,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self::join(client_id).auth_basic(username, password)
    }

    /// Create a JOIN message with API key authentication
    pub fn join_with_api_key(client_id: ClientId, api_key: impl Into<String>) -> Self {
        Self::join(client_id).auth_api_key(api_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::CLIENT_ID_UNASSIGNED;

    #[test]
    fn test_join_message_builder() {
        let message = MessageBuilder::join(CLIENT_ID_UNASSIGNED)
            .auth_token("test-token")
            .build()
            .unwrap();

        assert_eq!(message.message_type, MessageType::Join);
        assert_eq!(message.client_id, CLIENT_ID_UNASSIGNED);
        assert!(message.header.auth.is_some());
    }

    #[test]
    fn test_request_response_builder() {
        let request = MessageBuilder::simple_request(1000, 2000, "/api/test", "req-123")
            .build_with_payload(&serde_json::json!({"data": "test"}))
            .unwrap();

        assert_eq!(request.message_type, MessageType::Request);
        assert_eq!(request.client_id, 1000);

        let header = &request.header;
        assert!(header.routing.is_some());
        assert!(header.reqrep.is_some());

        let routing = header.routing.as_ref().unwrap();
        assert_eq!(routing[0].client_id, 2000);
        assert_eq!(routing[0].path, "/api/test");

        let reqrep = header.reqrep.as_ref().unwrap();
        assert_eq!(reqrep.req_type, RequestResponseType::Request);
        assert_eq!(reqrep.id, "req-123");
    }

    #[test]
    fn test_publish_subscribe_builder() {
        let publish = MessageBuilder::simple_publish(1000, "news.updates")
            .build_with_payload(&serde_json::json!({"title": "Breaking News"}))
            .unwrap();

        assert_eq!(publish.message_type, MessageType::Publish);
        assert_eq!(publish.header.topic, Some("news.updates".to_string()));

        let subscribe = MessageBuilder::simple_subscribe(2000, "news.updates").build().unwrap();

        assert_eq!(subscribe.message_type, MessageType::Subscribe);
        assert_eq!(subscribe.header.topic, Some("news.updates".to_string()));
    }

    #[test]
    fn test_ping_pong_builder() {
        let ping = MessageBuilder::ping_message(1000).build().unwrap();

        assert_eq!(ping.message_type, MessageType::Ping);
        assert!(ping.header.keepalive.is_some());

        let keepalive = ping.header.keepalive.as_ref().unwrap();
        let pong = MessageBuilder::pong_message(2000, keepalive.timestamp).build().unwrap();

        assert_eq!(pong.message_type, MessageType::Pong);
        let pong_keepalive = pong.header.keepalive.as_ref().unwrap();
        assert_eq!(pong_keepalive.timestamp, keepalive.timestamp);
    }

    #[test]
    fn test_validation() {
        // Valid request
        let valid_request = MessageBuilder::request(1000)
            .route_to(2000, "/api/test")
            .as_request("test-id")
            .build();
        assert!(valid_request.is_ok());

        // Invalid request (missing routing)
        let invalid_request = MessageBuilder::request(1000).as_request("test-id").build();
        assert!(invalid_request.is_err());

        // Invalid request (forbidden field)
        let invalid_request = MessageBuilder::request(1000)
            .route_to(2000, "/api/test")
            .as_request("test-id")
            .topic("forbidden")
            .build();
        assert!(invalid_request.is_err());
    }
}
