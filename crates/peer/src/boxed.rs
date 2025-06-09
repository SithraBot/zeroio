use std::{future::Future, pin::Pin};

use fleximq_protocol::Message;

use crate::{error::Error, extract::FromMessage, handler::Handler};

pub type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub type BoxedService<'a, Req, Rep, Err> =
    Box<dyn Fn(Req) -> BoxedFuture<'a, Result<Rep, Err>> + Send + 'a>;

pub fn boxed_service<'a, Req, Rep, Err, F>(f: F) -> BoxedService<'a, Req, Rep, Err>
where
    F: Fn(Req) -> BoxedFuture<'a, Result<Rep, Err>> + Send + 'a,
{
    Box::new(f)
}

pub type BoxedFleximqService<'a> = BoxedService<'a, &'a Message, Message, Error>;

pub fn handler_service<'a, H, Args>(handler: H) -> BoxedFleximqService<'a>
where
    H: Handler<Args> + Clone + Send + Sync,
    Args: FromMessage<'a>,
    H::Response: Into<Message>,
{
    boxed_service(move |message| {
        let handler = handler.clone();
        let fut = async move {
            // Process the arguments from the message first
            let args = match Args::from_message(message).await {
                Ok(args) => args,
                Err(e) => return Err(e.into()),
            };
            let response = handler.handle(args).await;
            Ok(response.into())
        };
        Box::pin(fut)
    })
}
