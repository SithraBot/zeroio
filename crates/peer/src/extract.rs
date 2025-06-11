use std::{
    convert::Infallible,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
};

use fleximq_protocol::{Header, Message, ProtocolError, types::BaseHeader};
use futures_util::{future, ready};
use pin_project::pin_project;
use serde::de::DeserializeOwned;

use crate::error::Error;

pub trait FromMessage<'a>: Sized {
    type Error: Into<Error>;
    type Future: Future<Output = Result<Self, Self::Error>> + Send + 'a;

    /// Create a new instance from a message asynchronously.
    fn from_message(message: &'a Message) -> Self::Future;
}

impl<'a, T> FromMessage<'a> for Option<T>
where
    T: FromMessage<'a>,
{
    type Error = Infallible;
    type Future = FromMessageOptionFuture<T::Future>;

    fn from_message(message: &'a Message) -> Self::Future {
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

impl<'a, T, E> FromMessage<'a> for Result<T, E>
where
    T: FromMessage<'a>,
    T::Error: Into<E>,
    E: Send + 'a,
{
    type Error = Infallible;
    type Future = FromMessageResultFuture<T::Future, E>;

    #[inline]
    fn from_message(message: &'a Message) -> Self::Future {
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

impl<'a> FromMessage<'a> for &'a Header {
    type Error = ProtocolError;
    type Future = future::Ready<Result<Self, Self::Error>>;

    fn from_message(message: &'a Message) -> Self::Future {
        let header = message.header();
        future::ready(header)
    }
}

pub struct Payload<'a, T>(&'a T)
where
    T: DeserializeOwned + Send + Sync + 'static;

impl<'a, T> Payload<'a, T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    pub const fn new(value: &'a T) -> Self {
        Payload(value)
    }
}

impl<T> Deref for Payload<'_, T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T> FromMessage<'a> for Payload<'a, T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    type Error = ProtocolError;
    type Future = future::Ready<Result<Self, Self::Error>>;

    fn from_message(message: &'a Message) -> Self::Future {
        let payload = message.payload_as::<T>();
        future::ready(payload.map(|value| Payload(value)))
    }
}

impl<'a> FromMessage<'a> for &'a rmpv::Value {
    type Error = ProtocolError;
    type Future = future::Ready<Result<Self, Self::Error>>;

    fn from_message(message: &'a Message) -> Self::Future {
        future::ready(message.payload())
    }
}

impl<'a> FromMessage<'a> for &'a BaseHeader {
    type Error = Infallible;
    type Future = future::Ready<Result<Self, Self::Error>>;

    fn from_message(message: &'a Message) -> Self::Future {
        future::ready(Ok(&message.base_header))
    }
}

mod tuple {
    #![allow(non_snake_case)]
    use super::{Context, Error, FromMessage, Infallible, Message, Pin, Poll, future, pin_project};
    #[pin_project(project = ExtractProject, project_replace = ExtractReplaceProject)]
    enum ExtractFuture<Fut, Res> {
        Future {
            #[pin]
            fut: Fut,
        },
        Done {
            output: Res,
        },
        Empty,
    }

    macro_rules! from_message_for_tuple {
        (@impl) => {
            impl FromMessage<'_> for () {
                type Error = Infallible;
                type Future = future::Ready<Result<Self, Self::Error>>;

                fn from_message(_: &Message) -> Self::Future {
                    future::ok(())
                }
            }
        };
        (@impl $first_fut:ident, $first_t:ident; $($rest_fut:ident, $rest_t:ident;)*)=>{
            from_message_for_tuple!(@inner $first_fut; $first_t $(, $rest_t)*);
            from_message_for_tuple!(@impl $($rest_fut, $rest_t;)*);
        };
        (@inner $fut:ident; $($T:ident),*)=>{
            #[allow(unused_parens)]
            impl<'a, $($T),+> FromMessage<'a> for ($($T,)+)
            where
                $($T: FromMessage<'a> + Send + 'a),+
            {
                type Error = Error;
                type Future = $fut<'a, $($T),+>;

                fn from_message(message: &'a Message) -> Self::Future {
                    $fut {
                        $(
                            $T: ExtractFuture::<$T::Future, $T>::Future {
                                fut: $T::from_message(message),
                            },
                        )+
                    }
                }
            }
            #[pin_project]
            pub struct $fut<'a, $($T: FromMessage<'a>),+> {
                $(
                    #[pin]
                    $T: ExtractFuture<$T::Future, $T>,
                )+
            }
            impl<'a, $($T: FromMessage<'a>),+> Future for $fut<'a, $($T),+>
            {
                type Output = Result<($($T,)+), Error>;

                fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                    let mut this = self.project();

                    let mut ready = true;
                    $(
                        match this.$T.as_mut().project() {
                            ExtractProject::Future { fut } => match fut.poll(cx) {
                                Poll::Ready(Ok(output)) => {
                                    let _ = this.$T.as_mut().project_replace(ExtractFuture::Done { output });
                                },
                                Poll::Ready(Err(err)) => return Poll::Ready(Err(err.into())),
                                Poll::Pending => ready = false,
                            },
                            ExtractProject::Done { .. } => {},
                            ExtractProject::Empty => unreachable!("FromMessage polled after finished"),
                        }
                    )+

                    if ready {
                        Poll::Ready(Ok(
                            ($(
                                match this.$T.project_replace(ExtractFuture::Empty) {
                                    ExtractReplaceProject::Done { output } => output,
                                    _ => unreachable!("FromMessage polled after finished"),
                                },
                            )+)
                        ))
                    } else {
                        Poll::Pending
                    }
                }
            }
        };
    }

    from_message_for_tuple!(@impl
        FutureTuple26, A;
        FutureTuple25, B;
        FutureTuple24, C;
        FutureTuple23, D;
        FutureTuple22, E;
        FutureTuple21, F;
        FutureTuple20, G;
        FutureTuple19, H;
        FutureTuple18, I;
        FutureTuple17, J;
        FutureTuple16, K;
        FutureTuple15, L;
        FutureTuple14, M;
        FutureTuple13, N;
        FutureTuple12, O;
        FutureTuple11, P;
        FutureTuple10, Q;
        FutureTuple9, R;
        FutureTuple8, S;
        FutureTuple7, T;
        FutureTuple6, U;
        FutureTuple5, V;
        FutureTuple4, W;
        FutureTuple3, X;
        FutureTuple2, Y;
        FutureTuple1, Z;
    );
}

pub use tuple::*;
