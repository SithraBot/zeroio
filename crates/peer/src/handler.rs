pub trait Handler<Args>: Clone + 'static {
    /// The type of response returned by the handler.
    type Response;
    /// The type of future returned by the handler.
    type Future: Future<Output = Self::Response>;

    /// The type of error returned by the handler.
    fn handle(&self, args: Args) -> Self::Future;
}

macro_rules! handler_for_tuple {
    (@impl) => {
        handler_for_tuple!(@inner);
    };
    (@impl $first:ident $(, $rest:ident)*)=>{
        handler_for_tuple!(@inner $first $(, $rest)*);
        handler_for_tuple!(@impl $($rest),*);
    };
    (@inner $($T:ident),*)=>{
        impl<Func, Fut, $($T),*> Handler<($($T,)*)> for Func
        where
            Fut: Future,
            Func: Fn($($T,)*) -> Fut + Clone + 'static,
        {
            type Future = Fut;
            type Response = Fut::Output;

            #[inline]
            #[allow(non_snake_case)]
            fn handle(&self, ($($T,)*): ($($T,)*) ) -> Self::Future {
                (self)($($T,)*)
            }
        }
    }
}
handler_for_tuple!(@impl A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z);
