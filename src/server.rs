use crate::{ConfigContext, DbHandle, Job, JobqMessage, Status};
use anyhow::Error;
use futures::{future, SinkExt, StreamExt, TryStreamExt};
use log::*;
use tmq::{router, Context, Message, Multipart};

pub async fn server(config: ConfigContext) -> Result<(), Error> {
    trace!("Connecting to db:{}", config.connect_url);
    let handle = DbHandle::new(&config.connect_url).await?;

    let (mut send, mut recv) = router(&Context::new())
        .bind(&config.job_address)?
        .split::<Multipart>();

    while let Some(msg) = recv.try_next().await? {
        let client_name = &msg[0];
        let client_msg: JobqMessage = serde_cbor::from_slice(&msg[1])?;

        match client_msg {
            JobqMessage::Hello => {
                send.send(
                    vec![
                        Message::from(&client_name as &[u8]),
                        JobqMessage::Hello.to_msg()?,
                    ]
                    .into(),
                )
                .await?;
            }
            JobqMessage::Request(job_request) => {
                let id = handle.submit_job_request(&job_request).await?;

                let job = Job {
                    id,
                    name: job_request.name,
                    uuid: job_request.uuid,
                    priority: job_request.priority,
                    params: job_request.params,
                    status: Status::Queued,
                };

                trace!("New Job:{:?}", job);
            }
            JobqMessage::Order(job) => {
                warn!("Received Order Message, ignoring:{:?}", job);
            }
            JobqMessage::Completed(job) => {
                debug!("Job completed:{}", job.id);
                handle.complete_job(job.id).await?;
            }
            JobqMessage::Failed(job, reason) => {
                warn!("Job failed: {}, Reason: {}", job.id, reason);
                handle.fail_job(job.id, reason).await?;
            }
        }

        let (queued, processing) =
            future::try_join(handle.get_queued(), handle.get_processing()).await?;

        if queued > 0 && processing < config.num as i64 {
            let jobs = handle
                .get_queued_jobs(config.num as i64 - processing as i64)
                .await?;

            for job in jobs {
                trace!("Submitting Job:{}", job.id);

                handle.begin_job(job.id).await?;

                send.send(
                    vec![
                        Message::from(job.name.as_bytes()),
                        JobqMessage::Order(job).to_msg()?,
                    ]
                    .into(),
                )
                .await?;
            }
        }
    }

    Ok(())
}
