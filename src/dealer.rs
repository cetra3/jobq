use tokio_serde::{formats::Cbor, Framed};

use crate::{ClientMessage, ServerMessage};
use anyhow::Error;
use futures::{Sink, Stream};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};

type ClientFramed = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    ClientMessage,
    ServerMessage,
    Cbor<ClientMessage, ServerMessage>,
>;

pub struct Dealer {
    connection: ClientFramed,
}

impl Dealer {
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        let tcp_stream = TcpStream::connect(addr).await?;

        let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());

        let connection = Framed::new(length_delimited, Cbor::default());

        Ok(Self { connection })
    }
}

impl Stream for Dealer {
    type Item = Result<ClientMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = &mut self.as_mut();

        match Pin::new(&mut self_mut.connection).poll_next(cx) {
            Poll::Ready(Some(val)) => Poll::Ready(Some(val.map_err(|err| err.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Sink<ServerMessage> for Dealer {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let self_mut = &mut self.as_mut();
        match Pin::new(&mut self_mut.connection).poll_ready(cx) {
            Poll::Ready(val) => Poll::Ready(val.map_err(|err| err.into())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: ServerMessage) -> Result<(), Self::Error> {
        let self_mut = &mut self.as_mut();
        Ok(Pin::new(&mut self_mut.connection).start_send(item)?)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let self_mut = &mut self.as_mut();
        match Pin::new(&mut self_mut.connection).poll_flush(cx) {
            Poll::Ready(val) => Poll::Ready(val.map_err(|err| err.into())),
            Poll::Pending => Poll::Pending,
        }
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let self_mut = &mut self.as_mut();
        match Pin::new(&mut self_mut.connection).poll_close(cx) {
            Poll::Ready(val) => Poll::Ready(val.map_err(|err| err.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}
