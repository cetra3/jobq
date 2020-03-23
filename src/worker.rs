use crate::{Job, JobqMessage};
use anyhow::Error;
use async_trait::async_trait;
use futures::Sink;
use futures::{channel::mpsc::unbounded, SinkExt, StreamExt, TryStreamExt};
use log::*;
use std::time::Duration;
use tmq::{dealer, Context, Multipart};
use tokio::time::delay_for;

#[async_trait]
pub trait Processor {
    const JOB_TYPE: &'static str;

    async fn process<S: Sink<JobqMessage, Error = Error> + Send>(
        &mut self,
        job: Job,
        send: S,
    ) -> Status;

    async fn work(&mut self, job_address: &str) -> Result<(), Error> {
        let job_type = Self::JOB_TYPE;
        debug!("Worker `{}` starting, sending hello", job_type);

        let (mut send_skt, mut recv) = dealer(&Context::new())
            .set_identity(job_type.as_bytes())
            .connect(&job_address)?
            .split::<Multipart>();

        let (mut send, mut recv_skt) = unbounded::<JobqMessage>();

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
                if let Err(err) = ping_sender.send(JobqMessage::Hello).await {
                    error!("Error:{}", err);
                };
                delay_for(Duration::from_millis(10000)).await;
            }
        });

        loop {
            if let Some(srv_msg) = recv.try_next().await? {
                match serde_cbor::from_slice(&srv_msg[0]) {
                    Ok(JobqMessage::Order(job)) => {
                        match self.process(job, send.clone().sink_err_into()).await {
                            Status::Deferred => (),
                            Status::Completed(job) => {
                                send.send(JobqMessage::Completed(job)).await?;
                            }
                            Status::Failed(job, reason) => {
                                send.send(JobqMessage::Failed(job, reason)).await?;
                            }
                        }
                    }
                    Ok(JobqMessage::Hello) => {
                        debug!("Pong: {}", job_type);
                    }
                    Ok(_) => (),
                    Err(err) => {
                        error!("Error decoding message:{}", err);
                    }
                }
            }
        }
    }
}

pub enum Status {
    Deferred, //If you want to defer the status due to sending the completed job yourself later
    Completed(Job),
    Failed(Job, String),
}

pub struct TestWorker;

#[async_trait]
impl Processor for TestWorker {
    const JOB_TYPE: &'static str = "test";

    async fn process<S: Sink<JobqMessage, Error = Error> + Send>(
        &mut self,
        job: Job,
        _send: S,
    ) -> Status {
        delay_for(Duration::from_millis(10000)).await;
        Status::Completed(job)
    }
}

pub async fn worker<P: Processor>(job_address: &str, mut worker: P) -> Result<(), Error> {
    trace!("Worker `{}` starting, sending hello", P::JOB_TYPE);

    let (mut send_skt, mut recv) = dealer(&Context::new())
        .set_identity(P::JOB_TYPE.as_bytes())
        .connect(&job_address)?
        .split::<Multipart>();

    let (mut send, mut recv_skt) = unbounded::<JobqMessage>();

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
            if let Err(err) = ping_sender.send(JobqMessage::Hello).await {
                error!("Error:{}", err);
            };
            delay_for(Duration::from_millis(10000)).await;
        }
    });

    loop {
        if let Some(srv_msg) = recv.try_next().await? {
            match serde_cbor::from_slice(&srv_msg[0]) {
                Ok(JobqMessage::Order(job)) => {
                    match worker.process(job, send.clone().sink_err_into()).await {
                        Status::Deferred => (),
                        Status::Completed(job) => {
                            send.send(JobqMessage::Completed(job)).await?;
                        }
                        Status::Failed(job, reason) => {
                            send.send(JobqMessage::Failed(job, reason)).await?;
                        }
                    }
                }
                Ok(JobqMessage::Hello) => {
                    debug!("Pong: {}", P::JOB_TYPE);
                }
                Ok(_) => (),
                Err(err) => {
                    error!("Error decoding message:{}", err);
                }
            }
        }
    }
}
