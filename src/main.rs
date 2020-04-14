use anyhow::{anyhow, Error};
use futures::{SinkExt, StreamExt, TryStream, TryStreamExt};
use log::*;
use serde_json::Value;
use std::env;
use structopt::StructOpt;
use tmq::{dealer, Context, Multipart, TmqError};
use uuid::Uuid;

use std::time::Duration;
use tokio::time::delay_for;

use jobq::server::Server;
use jobq::worker::{Worker, TestWorker};
use jobq::{ClientMessage, JobRequest, Priority, ServerMessage, ToMpart};

#[derive(StructOpt, Clone, Debug, PartialEq)]
#[structopt(name = "jobq", about = "ZeroMQ Job Queue")]
pub struct ConfigContext {
    #[structopt(
        short = "c",
        long = "connect_url",
        help = "PostgreSQL Connection URL",
        default_value = "postgres://jobq:jobq@127.0.0.1"
    )]
    connect_url: String,

    #[structopt(
        short = "l",
        long = "listen_address",
        help = "Jobq Listen Address",
        default_value = "tcp://127.0.0.1:8888"
    )]
    job_address: String,
    #[structopt(
        short = "n",
        long = "number_active",
        help = "Number of Active Jobs in Parallel",
        default_value = "4"
    )]
    num: usize,
}

async fn get_message<S: TryStream<Ok = Multipart, Error = TmqError> + Unpin>(
    recv: &mut S,
) -> Result<ClientMessage, Error> {
    if let Some(msg) = recv.try_next().await? {
        let jobq_message: ClientMessage = serde_cbor::from_slice(&msg[0])?;

        Ok(jobq_message)
    } else {
        Err(anyhow!("No Messages in Stream"))
    }
}

async fn setup() -> Result<(), Error> {
    let config = ConfigContext::from_args();

    let server = Server::new(
        config.connect_url.clone(),
        config.job_address.clone(),
        config.num,
    );

    tokio::spawn(async move {
        if let Err(err) = server.serve().await {
            error!("{}", err);
        }
    });

    delay_for(Duration::from_secs(1)).await;

    let worker_config = config.clone();

    tokio::spawn(async move {
        if let Err(err) = TestWorker.work(&worker_config.job_address).await {
            error!("{}", err);
        }
    });

    let (mut send, mut recv) = dealer(&Context::new())
        .set_identity(b"test_client")
        .connect(&config.job_address)?
        .split::<Multipart>();

    //Send hello
    send.send(ClientMessage::Hello.to_mpart()?).await?;

    if let ClientMessage::Hello = get_message(&mut recv).await? {
        debug!("Received Hello response, sending a couple of jobs");

        for i in 0..500 {
            let priority = if i % 2 == 0 {
                Priority::High
            } else {
                Priority::Normal
            };

            let job = JobRequest {
                name: "test".into(),
                username: "test_client".into(),
                params: Value::Null,
                uuid: Uuid::new_v4(),
                priority,
            };

            send.send(ServerMessage::Request(job).to_mpart()?).await?;
        }

        debug!("Done!");
    }

    loop {
        let message = get_message(&mut recv).await?;

        debug!("Message:{:?}", message);
    }
}

#[tokio::main]
async fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "jobq=DEBUG");
    }

    pretty_env_logger::init_timed();

    if let Err(err) = setup().await {
        error!("{}", err);
    }
}
