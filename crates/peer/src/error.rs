use std::convert::Infallible;

use fleximq_protocol::ProtocolError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("Message does not contain payload")]
    NoPayload,
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}
