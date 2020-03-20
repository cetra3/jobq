use crate::get_message;
use crate::{ConfigContext, JobqMessage};
use anyhow::Error;
use futures::{SinkExt, StreamExt};
use log::*;
//use std::time::Duration;
use tmq::{dealer, Context, Multipart};
//use tokio::time::delay_for;

pub async fn worker(config: ConfigContext, name: &str) -> Result<(), Error> {
    trace!("Worker `{}` starting, sending hello", name);

    let (mut send, mut recv) = dealer(&Context::new())
        .set_identity(name.as_bytes())
        .connect(&config.job_address)?
        .split::<Multipart>();

    //Send hello
    //send.send(JobqMessage::Hello.to_mpart()?).await?;

    loop {
        let message = get_message(&mut recv).await?;
        match message {
            JobqMessage::Order(job) => {
                trace!("Order received:{}", job.id);

                //delay_for(Duration::from_secs(1)).await;

                if job.id % 4 == 0{
                    trace!("Simulating failure for id:{}", job.id);
                    send.send(JobqMessage::Failed(job, "Simulating Failure".into()).to_mpart()?).await?;
                } else {

                    trace!("Order completed:{}", job.id);
                    send.send(JobqMessage::Completed(job).to_mpart()?).await?;
                }

            }
            _ => (),
        }
    }
}
