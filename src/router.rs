use std::collections::HashMap;
use tokio_serde::{formats::Cbor, Framed};

use crate::{ClientMessage, ServerMessage};
use anyhow::{anyhow, Error};
use futures::{SinkExt, Stream};
use log::*;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};

type ServerFramed = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    ServerMessage,
    ClientMessage,
    Cbor<ServerMessage, ClientMessage>,
>;

pub struct Router {
    clients: HashMap<String, ServerFramed>,
    pending_clients: Vec<ServerFramed>,
    listener: TcpListener,
    buffer: Vec<ServerMessage>,
}

impl Router {
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;

        Ok(Self {
            clients: HashMap::new(),
            listener,
            buffer: Vec::new(),
            pending_clients: Vec::new(),
        })
    }

    pub async fn send_message(&mut self, client: &str, msg: ClientMessage) -> Result<(), Error> {
        if let Some(connection) = self.clients.get_mut(client) {
            connection.send(msg).await?;
        } else {
            return Err(anyhow!("Client `{}` not connected!", client));
        }

        Ok(())
    }

    pub fn is_connected(&self, client: &str) -> bool {
        self.clients.contains_key(client)
    }
}

impl Stream for Router {
    type Item = Result<ServerMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = &mut self.as_mut();

        if let Poll::Ready(val) = Pin::new(&mut self_mut.listener).poll_next(cx) {
            match val {
                Some(Ok(tcp_stream)) => {
                    let length_delimited =
                        CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());

                    let framed = Framed::new(length_delimited, Cbor::default());

                    self_mut.pending_clients.push(framed);
                }
                Some(Err(err)) => {
                    error!("Error checking for new requests:{:?}", err);
                }
                None => {
                    //TCP Listener is dead
                    return Poll::Ready(None);
                }
            }
        }

        let mut new_pending = Vec::new();

        mem::swap(&mut self_mut.pending_clients, &mut new_pending);

        for mut pending in new_pending.into_iter() {
            if let Poll::Ready(val) = Pin::new(&mut pending).poll_next(cx) {
                match val {
                    Some(Ok(ServerMessage::Hello(name))) => {
                        debug!("New Client connection from `{}`", name);
                        self_mut.buffer.push(ServerMessage::Hello(name.clone()));
                        self_mut.clients.insert(name, pending);
                    }
                    Some(Ok(msg)) => {
                        warn!("Received unknown message during handshake:{:?}", msg);
                    }
                    Some(Err(err)) => {
                        error!("Error checking for new requests:{:?}", err);
                    }
                    None => (),
                }
            } else {
                self_mut.pending_clients.push(pending);
            }
        }

        let mut new_clients = HashMap::new();

        mem::swap(&mut self_mut.clients, &mut new_clients);

        for (name, mut client) in new_clients.into_iter() {
            match Pin::new(&mut client).poll_next(cx) {
                Poll::Ready(Some(Ok(val))) => {
                    trace!("Received message from `{}`: {:?}", name, val);
                    self_mut.buffer.push(val);
                    self_mut.clients.insert(name, client);
                }
                Poll::Ready(None) => {
                    //Finished
                    debug!("Client `{}` disconnecting", name);
                }
                Poll::Ready(Some(Err(err))) => {
                    //Error
                    error!("Error from `{}`: {} Removing connection.", name, err);
                }
                Poll::Pending => {
                    self_mut.clients.insert(name, client);
                }
            }
        }


        if let Some(val) = self_mut.buffer.pop() {
            if self_mut.buffer.len() > 0 {
                cx.waker().wake_by_ref();
            }
            return Poll::Ready(Some(Ok(val)));
        }

        return Poll::Pending;
    }
}
