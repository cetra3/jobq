use crate::{dealer::Dealer, ClientMessage, Job, ServerMessage};
use anyhow::{anyhow, Error};
use async_trait::async_trait;
use futures::{channel::mpsc::unbounded, future::ready, SinkExt, StreamExt};
use log::*;
use std::time::Duration;
use tokio::time::delay_for;

#[async_trait]
pub trait Worker: Sized {
    const JOB_NAME: &'static str;

    async fn process(&self, job: Job) -> Result<(), Error>;

    async fn work(&self, job_address: &str) -> Result<(), Error> {
        let job_type = Self::JOB_NAME;
        debug!("Worker `{}` starting, sending hello", job_type);

        let (mut send_skt, recv) = Dealer::new(&job_address).await?.split();

        let (send, mut recv_skt) = unbounded::<ServerMessage>();

        tokio::spawn(async move {
            while let Some(jobq_message) = recv_skt.next().await {
                if let Err(err) = send_skt.send(jobq_message).await {
                    error!("Error sending message:{}", err);
                }
            }
        });

        let mut ping_sender = send.clone();

        tokio::spawn(async move {
            loop {
                if let Err(err) = ping_sender
                    .send(ServerMessage::Hello(job_type.to_string()))
                    .await
                {
                    error!("Error:{}", err);
                };
                delay_for(Duration::from_millis(10000)).await;
            }
        });

        recv.filter_map(|val| {
            match val {
                Ok(ClientMessage::Order(job)) => return ready(Some(job)),
                Ok(ClientMessage::Hello(name)) => {
                    debug!("Pong: {}", name);
                }
                Err(err) => {
                    error!("Error decoding message:{}", err);
                }
                _ => (),
            }

            return ready(None);
        })
        .map(|job| (self.process(job.clone()), send.clone(), job))
        .for_each_concurrent(None, |(status, mut send, job)| async move {
            let server_message = match status.await {
                Ok(()) => ServerMessage::Completed(job),
                Err(err) => ServerMessage::Failed(job, err.to_string()),
            };

            if let Err(err) = send.send(server_message).await {
                error!("Error sending server message: {}", err);
            }
        })
        .await;

        Ok(())
    }
}
pub struct TestWorker;

#[async_trait]
impl Worker for TestWorker {
    const JOB_NAME: &'static str = "test";

    async fn process(&self, job: Job) -> Result<(), Error> {
        delay_for(Duration::from_millis(100)).await;
        if job.id % 12 == 0 {
            return Err(anyhow!("Simulating failure"));
        }

        Ok(())
    }
}
