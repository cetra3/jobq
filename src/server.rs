use crate::{db::DbHandle, ClientMessage, Job, ServerMessage, Status, ToMpart, WorkerMessage};
use anyhow::Error;
use futures::{Sink, SinkExt, StreamExt, TryStreamExt};
use log::*;
use tmq::TmqError;
use tmq::{router, Context, Message, Multipart};

pub struct Server {
    connect_url: String,
    job_address: String,
    num: usize,
}

impl Server {
    pub fn new(connect_url: String, job_address: String, num: usize) -> Self {
        Server {
            connect_url,
            job_address,
            num,
        }
    }
}

impl Server {
    pub async fn serve(&self) -> Result<(), Error> {
        trace!("Connecting to db:{}", self.connect_url);
        let handle = DbHandle::new(&self.connect_url).await?;

        let (mut send, mut recv) = router(&Context::new())
            .bind(&self.job_address)?
            .split::<Multipart>();

        //Resubmit processing jobs
        let mut processing = handle.get_processing_jobs().await?;

        let mut active = processing.len();

        while let Some(msg) = recv.try_next().await? {
            let client_name = &msg[0];
            let server_msg = serde_cbor::from_slice::<ServerMessage>(&msg[1]);

            trace!("Active Jobs:{}", active);

            match server_msg {
                Ok(ServerMessage::Hello) => {
                    if let Some(name) = client_name.as_str() {
                        debug!("Ping: {}", name);

                        //Drain out existing processing jobs
                        let (jobs, outstanding): (Vec<Job>, Vec<Job>) =
                            processing.into_iter().partition(|job| job.name == name);

                        processing = outstanding;

                        for job in jobs {
                            send_job(&handle, job, &mut send).await?;
                        }
                    }

                    send.send(
                        vec![
                            Message::from(&client_name as &[u8]),
                            ClientMessage::Hello.to_msg()?,
                        ]
                        .into(),
                    )
                    .await?;
                }
                Ok(ServerMessage::Request(job_request)) => {
                    let id = handle.submit_job_request(&job_request).await?;

                    let job = Job {
                        id,
                        name: job_request.name,
                        username: job_request.username,
                        uuid: job_request.uuid,
                        priority: job_request.priority,
                        params: job_request.params,
                        status: Status::Queued,
                    };

                    debug!("New: {:?}", job);
                }
                Ok(ServerMessage::Completed(job)) => {
                    trace!("Job completed:{}", job.id);
                    handle.complete_job(job.id).await?;
                    active = active - 1;
                }
                Ok(ServerMessage::Failed(job, reason)) => {
                    warn!("Job failed: {}, Reason: {}", job.id, reason);
                    handle.fail_job(job.id, reason).await?;
                    active = active - 1;
                }
                Err(err) => {
                    warn!("Could not deserialize message:{}", err);
                }
            }

            //If we have less active tasks lets check the queued stuff
            if active < self.num {
                let jobs = handle
                    .get_queued_jobs(self.num as i64 - active as i64)
                    .await?;

                for job in jobs {
                    send_job(&handle, job, &mut send).await?;
                    active = active + 1;
                }
            }
        }

        Ok(())
    }
}

async fn send_job<S: Sink<Multipart, Error = TmqError> + Unpin>(
    handle: &DbHandle,
    job: Job,
    send: &mut S,
) -> Result<(), Error> {
    handle.begin_job(job.id).await?;

    send.send(
        vec![
            Message::from(job.name.as_bytes()),
            WorkerMessage::Order(job).to_msg()?,
        ]
        .into(),
    )
    .await?;
    Ok(())
}
