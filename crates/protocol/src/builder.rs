//! Fluent builder API for constructing protocol messages

use crate::{
    errors::ProtocolResult,
    message::{Message, NIL, RawMessage},
    types::{
        Auth, ClientId, Header, MessageType, RequestResponse, RequestResponseType, Routing,
        StatusCode,
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
        Self {
            message_type,
            client_id,
            header: Header::new(),
        }
    }

    /// Set routing information directly
    #[must_use]
    pub fn set_routing(mut self, routing: Vec<Routing>) -> Self {
        self.header.routing = Some(routing);
        self
    }

    /// Set routing target directly
    #[must_use]
    pub fn set_routing_target(
        mut self,
        client_name: impl Into<String>,
        path: impl Into<String>,
        client_id: Option<ClientId>,
    ) -> Self {
        self.header.routing = Some(vec![Routing {
            client_name: client_name.into(),
            path: path.into(),
            client_id,
        }]);
        self
    }

    /// Add a route with `client_name`, path, and `client_id` to the header
    #[must_use]
    pub fn route_with_id(
        mut self,
        client_name: impl Into<String>,
        path: impl Into<String>,
        client_id: ClientId,
    ) -> Self {
        self.header = self.header.with_route_and_id(client_name, path, client_id);
        self
    }

    /// Set a single routing target
    #[must_use]
    pub fn route_to(
        mut self,
        client_name: impl Into<String>,
        path: impl Into<String>,
        client_id: Option<ClientId>,
    ) -> Self {
        self.header.routing = Some(vec![Routing {
            client_name: client_name.into(),
            path: path.into(),
            client_id,
        }]);
        self
    }

    /// Set multiple routing targets
    #[must_use]
    pub fn route_to_many<I, S, T>(mut self, targets: I) -> Self
    where
        I: IntoIterator<Item = (S, T, Option<ClientId>)>,
        S: Into<String>,
        T: Into<String>,
    {
        let routing = targets
            .into_iter()
            .map(|(client_name, path, client_id)| Routing {
                client_name: client_name.into(),
                path: path.into(),
                client_id,
            })
            .collect();
        self.header.routing = Some(routing);
        self
    }

    /// Forward to another client
    #[must_use]
    pub fn forward_to(
        mut self,
        message: &Message,
        client_name: impl Into<String>,
        path: impl Into<String>,
        client_id: Option<ClientId>,
    ) -> Self {
        // Copy reqrep if available
        if let Ok(header) = message.header() {
            if let Some(reqrep) = &header.reqrep {
                self.header.reqrep = Some(reqrep.clone());
            }
        }

        // Set routing
        let routing = vec![Routing {
            client_name: client_name.into(),
            path: path.into(),
            client_id,
        }];
        self.header.routing = Some(routing);

        self
    }

    /// Set request/response correlation
    #[must_use]
    pub fn reqrep(mut self, reqrep: RequestResponse) -> Self {
        self.header.reqrep = Some(reqrep);
        self
    }

    /// Add request type and ID
    #[must_use]
    pub fn as_request(mut self, request_id: impl Into<String>) -> Self {
        self.header.reqrep = Some(RequestResponse {
            req_type: RequestResponseType::Request,
            id:       request_id.into(),
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
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.header.topic = Some(topic.into());
        self
    }

    /// Set status code
    #[must_use]
    pub const fn status(mut self, status: StatusCode) -> Self {
        self.header.status = Some(status);
        self
    }

    /// Set status code (alias for status)
    #[must_use]
    pub const fn with_status(self, status: StatusCode) -> Self {
        self.status(status)
    }

    /// Set authentication
    #[must_use]
    pub fn auth(mut self, auth: Auth) -> Self {
        self.header.auth = Some(auth);
        self
    }

    /// Add authentication with a token
    #[must_use]
    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.header = self.header.with_auth(Auth::token(token));
        self
    }

    /// Set token authentication
    #[must_use]
    pub fn auth_token_legacy(mut self, token: impl Into<String>) -> Self {
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

    /// Add a client name to message header
    #[must_use]
    pub fn with_client_name(mut self, client_name: impl Into<String>) -> Self {
        self.header.client_name = Some(client_name.into());
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

        RawMessage::new(self.message_type, self.client_id, self.header, &NIL)
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

        RawMessage::new(self.message_type, self.client_id, self.header, payload)
    }
}

/// Specialized builders for different message types
impl MessageBuilder {
    /// Create a JOIN message builder
    ///
    /// # Arguments
    ///
    /// * `client_id` - The client ID (should be UNASSIGNED for initial JOIN)
    /// * `client_name` - The logical service name
    #[must_use]
    pub fn join(client_id: ClientId, client_name: impl Into<String>) -> Self {
        Self::new(MessageType::Join, client_id).with_client_name(client_name)
    }

    /// Create a REQUEST message builder
    #[must_use]
    pub fn request(
        client_id: ClientId,
        target_client_name: impl Into<String>,
        path: impl Into<String>,
        target_id: Option<ClientId>,
    ) -> Self {
        let mut builder = Self::new(MessageType::Request, client_id);
        builder = builder.route_to(target_client_name, path, target_id);
        builder
    }

    /// Create a NOTIFICATION message builder
    #[must_use]
    pub fn notification(
        client_id: ClientId,
        target_name: impl Into<String>,
        path: impl Into<String>,
        target_id: Option<ClientId>,
    ) -> Self {
        Self::new(MessageType::Notification, client_id).route_to(target_name, path, target_id)
    }

    /// Create a BROADCAST message builder
    #[must_use]
    pub fn broadcast(client_id: ClientId) -> Self {
        Self::new(MessageType::Broadcast, client_id)
    }

    /// Create a subscribe message
    #[must_use]
    pub fn subscribe(client_id: ClientId) -> Self {
        Self::new(MessageType::Subscribe, client_id)
    }

    /// Create a RESPONSE message builder with routing and request correlation
    #[must_use]
    pub fn response(
        client_id: ClientId,
        target_name: impl Into<String>,
        request_id: impl Into<String>,
        path: impl Into<String>,
        target_id: ClientId,
    ) -> Self {
        Self::new(MessageType::Response, client_id)
            .route_with_id(target_name, path, target_id)
            .as_response(request_id)
    }

    /// Create a PUBLISH message builder
    #[must_use]
    pub fn publish(client_id: ClientId) -> Self {
        Self::new(MessageType::Publish, client_id)
    }

    /// Create a UNSUBSCRIBE message builder
    #[must_use]
    pub fn unsubscribe(client_id: ClientId) -> Self {
        Self::new(MessageType::Unsubscribe, client_id)
    }

    /// Create a forwarded response message with status and correlation ID
    #[must_use]
    pub fn forwarded_response_with_status(
        client_id: ClientId,
        requester_name: impl Into<String>,
        request_id: impl Into<String>,
        path: impl Into<String>,
        requester_id: ClientId,
    ) -> Self {
        Self::new(MessageType::Response, client_id)
            .route_to(requester_name, path, Some(requester_id)) // client_id is required for response messages
            .as_response(request_id)
    }

    /// Create a response from request with optional status
    #[must_use]
    pub fn response_from_request(
        client_id: ClientId,
        message: &Message,
        status: Option<StatusCode>,
    ) -> Self {
        let mut builder = Self::new(MessageType::Response, client_id);
        if let Ok(header) = message.header() {
            if let Some(reqrep) = &header.reqrep {
                builder = builder.reqrep(reqrep.clone());
            }
        }
        if let Some(status) = status {
            builder = builder.status(status);
        }
        builder
    }

    /// Create a simple notification message
    #[must_use]
    pub fn simple_notification(
        client_id: ClientId,
        target_name: impl Into<String>,
        path: impl Into<String>,
        target_id: Option<ClientId>,
    ) -> Self {
        Self::new(MessageType::Notification, client_id).route_to(target_name, path, target_id)
    }

    /// Create a simple broadcast message
    #[must_use]
    pub fn simple_broadcast(client_id: ClientId, topic: impl Into<String>) -> Self {
        Self::broadcast(client_id).with_topic(topic)
    }

    /// Create a simple publish message
    #[must_use]
    pub fn simple_publish(client_id: ClientId, topic: impl Into<String>) -> Self {
        Self::publish(client_id).with_topic(topic)
    }

    /// Create a simple subscribe message
    #[must_use]
    pub fn simple_subscribe(client_id: ClientId, topic: impl Into<String>) -> Self {
        Self::subscribe(client_id).with_topic(topic)
    }

    /// Create a JOIN message with token authentication
    #[must_use]
    pub fn join_with_token(
        client_id: ClientId,
        client_name: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self::join(client_id, client_name).auth_token(token)
    }

    /// Create a JOIN message with basic authentication
    #[must_use]
    pub fn join_with_basic_auth(
        client_id: ClientId,
        client_name: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self::join(client_id, client_name).auth_basic(username, password)
    }

    /// Create a JOIN message with API key authentication
    #[must_use]
    pub fn join_with_api_key(
        client_id: ClientId,
        client_name: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self::join(client_id, client_name).auth_api_key(api_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::CLIENT_ID_UNASSIGNED;

    #[test]
    fn test_join_message_builder() {
        let message = MessageBuilder::join(CLIENT_ID_UNASSIGNED, "test-client")
            .auth_token("test-token")
            .build()
            .unwrap();

        assert_eq!(message.message_type, MessageType::Join);
        assert_eq!(message.client_id, CLIENT_ID_UNASSIGNED);
        assert!(message.header.auth.is_some());
        assert_eq!(message.header.client_name, Some("test-client".to_string()));
    }

    #[test]
    fn test_response_message_builder() {
        let message =
            MessageBuilder::response(2000, "requester-service", "test-id", "/api/test", 1000)
                .with_status(StatusCode::OK)
                .build()
                .unwrap();

        assert_eq!(message.message_type, MessageType::Response);
        assert_eq!(message.client_id, 2000);

        // Check routing
        let routing = message.header.routing.unwrap();
        assert_eq!(routing.len(), 1);
        assert_eq!(routing[0].client_name, "requester-service");
        assert_eq!(routing[0].client_id, Some(1000)); // client_id is required for response messages
        assert_eq!(routing[0].path, "/api/test");

        // Check reqrep
        let reqrep = message.header.reqrep.unwrap();
        assert_eq!(reqrep.req_type, RequestResponseType::Correlation);
        assert_eq!(reqrep.id, "test-id");

        // Check status
        assert_eq!(message.header.status, Some(StatusCode::OK));
    }

    #[test]
    fn test_publish_subscribe_builder() {
        let publish = MessageBuilder::new(MessageType::Publish, 1000)
            .with_topic("news.updates")
            .build_with_payload(&serde_json::json!({"title": "Breaking News"}))
            .unwrap();

        assert_eq!(publish.message_type, MessageType::Publish);
        assert_eq!(publish.header.topic, Some("news.updates".to_string()));

        let subscribe = MessageBuilder::new(MessageType::Subscribe, 2000)
            .with_topic("news.updates")
            .build()
            .unwrap();

        assert_eq!(subscribe.message_type, MessageType::Subscribe);
        assert_eq!(subscribe.header.topic, Some("news.updates".to_string()));
    }

    #[test]
    fn test_validation() {
        // Valid request
        let valid_request = MessageBuilder::request(1000, "service", "/api/test", Some(2000))
            .as_request("test-id")
            .build();
        assert!(valid_request.is_ok());

        // Invalid request (missing routing)
        let invalid_request =
            MessageBuilder::new(MessageType::Request, 1000).as_request("test-id").build();
        assert!(invalid_request.is_err());

        // Invalid request (forbidden field)
        let invalid_request = MessageBuilder::request(1000, "service", "/api/test", Some(2000))
            .as_request("test-id")
            .with_topic("forbidden")
            .build();
        assert!(invalid_request.is_err());
    }
}
