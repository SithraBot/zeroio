use std::{
    convert::Infallible,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
};

use fleximq_protocol::{Header, Message, ProtocolError};
use futures_util::{future, ready};
use pin_project::pin_project;

use crate::error::Error;
pub trait FromMessage: Sized {
    type Error: Into<Error>;
    type Future: Future<Output = Result<Self, Self::Error>>;

    /// Create a new instance from a message asynchronously.
    fn from_message(message: &Message) -> Self::Future;
}

impl<T> FromMessage for Option<T>
where
    T: FromMessage,
{
    type Error = Infallible;
    type Future = FromMessageOptionFuture<T::Future>;

    fn from_message(message: &Message) -> Self::Future {
        FromMessageOptionFuture {
            future: T::from_message(message),
        }
    }
}

#[pin_project]
pub struct FromMessageOptionFuture<Fut> {
    #[pin]
    future: Fut,
}

impl<Fut, T, E> Future for FromMessageOptionFuture<Fut>
where
    Fut: Future<Output = Result<T, E>>,
    E: Into<Error>,
{
    type Output = Result<Option<T>, Infallible>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.future.poll(cx));
        match res {
            Ok(t) => Poll::Ready(Ok(Some(t))),
            Err(_err) => Poll::Ready(Ok(None)),
        }
    }
}

impl<T, E> FromMessage for Result<T, E>
where
    T: FromMessage,
    T::Error: Into<E>,
{
    type Error = Infallible;
    type Future = FromMessageResultFuture<T::Future, E>;

    #[inline]
    fn from_message(message: &Message) -> Self::Future {
        FromMessageResultFuture {
            fut:      T::from_message(message),
            _phantom: PhantomData,
        }
    }
}

#[pin_project]
pub struct FromMessageResultFuture<Fut, E> {
    #[pin]
    fut:      Fut,
    _phantom: PhantomData<E>,
}

impl<Fut, T, Ei, E> Future for FromMessageResultFuture<Fut, E>
where
    Fut: Future<Output = Result<T, Ei>>,
    Ei: Into<E>,
{
    type Output = Result<Result<T, E>, Infallible>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.fut.poll(cx));
        Poll::Ready(Ok(res.map_err(Into::into)))
    }
}

impl FromMessage for Header {
    type Error = ProtocolError;
    type Future = future::Ready<Result<Self, Self::Error>>;

    fn from_message(message: &Message) -> Self::Future {
        let header = message.header();
        future::ready(header)
    }
}

pub struct Payload<T>(T);

impl<T> Payload<T> {
    pub fn new(value: T) -> Self {
        Payload(value)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for Payload<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Payload<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
