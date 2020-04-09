use crate::{Job, ServerMessage, ToMpart, WorkerMessage};
use anyhow::Error;
use async_trait::async_trait;
use futures::{channel::mpsc::unbounded, future::ready, SinkExt, StreamExt};
use log::*;
use std::time::Duration;
use tmq::{dealer, Context, Multipart};
use tokio::time::delay_for;

#[async_trait]
pub trait Processor: Sized {
    const JOB_TYPE: &'static str;

    async fn process(&self, job: Job) -> Status;

    async fn work(&self, job_address: &str) -> Result<(), Error> {
        let job_type = Self::JOB_TYPE;
        debug!("Worker `{}` starting, sending hello", job_type);

        let (mut send_skt, recv) = dealer(&Context::new())
            .set_identity(job_type.as_bytes())
            .connect(&job_address)?
            .split::<Multipart>();

        let (send, mut recv_skt) = unbounded::<ServerMessage>();

        tokio::spawn(async move {
            while let Some(jobq_message) = recv_skt.next().await {
                if let Ok(msg) = jobq_message.to_mpart() {
                    if let Err(err) = send_skt.send(msg).await {
                        error!("Error sending message:{}", err);
                    }
                }
            }
        });

        let mut ping_sender = send.clone();

        tokio::spawn(async move {
            loop {
                if let Err(err) = ping_sender.send(ServerMessage::Hello).await {
                    error!("Error:{}", err);
                };
                delay_for(Duration::from_millis(10000)).await;
            }
        });

        recv.filter_map(|val| {
            match val
                .map_err(Error::from)
                .and_then(|msg| serde_cbor::from_slice(&msg[0]).map_err(Error::from))
            {
                Ok(WorkerMessage::Order(job)) => return ready(Some(job)),
                Ok(WorkerMessage::Hello) => {
                    debug!("Pong: {}", job_type);
                }
                Err(err) => {
                    error!("Error decoding message:{}", err);
                }
            }

            return ready(None);
        })
        .map(|job| (self.process(job), send.clone()))
        .for_each_concurrent(None, |(status, mut send)| async move {
            match status.await {
                Status::Completed(job) => {
                    if let Err(err) = send.send(ServerMessage::Completed(job)).await {
                        error!("Error sending completed message: {}", err);
                    }
                }
                Status::Failed(job, reason) => {
                    if let Err(err) = send.send(ServerMessage::Failed(job, reason)).await {
                        error!("Error sending failed message:{}", err);
                    }
                }
            }
        })
        .await;

        Ok(())
    }
}

pub enum Status {
    Completed(Job),
    Failed(Job, String),
}

pub struct TestWorker;

#[async_trait]
impl Processor for TestWorker {
    const JOB_TYPE: &'static str = "test";

    async fn process(&self, job: Job) -> Status {
        delay_for(Duration::from_millis(100)).await;
        if job.id % 12 == 0 {
            Status::Failed(job, "Simulating failure".into())
        } else {
            Status::Completed(job)
        }
    }
}
