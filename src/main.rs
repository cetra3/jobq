use anyhow::Error;
use futures::{SinkExt, StreamExt, TryStreamExt};
use log::*;
use serde_json::Value;
use std::env;
use structopt::StructOpt;
use uuid::Uuid;

use std::time::Duration;
use tokio::time::delay_for;

use jobq::server::Server;
use jobq::worker::{TestWorker, Worker};
use jobq::{dealer::Dealer, ClientMessage, JobRequest, Priority, ServerMessage};

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
        default_value = "127.0.0.1:8888"
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

async fn setup() -> Result<(), Error> {
    let config = ConfigContext::from_args();

    let server = Server::new(
        config.connect_url.clone(),
        config.job_address.clone(),
        config.num,
    );

    tokio::spawn(async move {
        if let Err(err) = server.serve().await {
            error!("Error starting server: {}", err);
        }
    });

    delay_for(Duration::from_secs(1)).await;

    let worker_config = config.clone();

    tokio::spawn(async move {
        if let Err(err) = TestWorker.work(&worker_config.job_address).await {
            error!("Error starting worker: {}", err);
        }
    });

    let (mut send, mut recv) = Dealer::new(&config.job_address).await?.split();

    //Send hello
    send.send(ServerMessage::Hello("Test Client".into()))
        .await?;
    
    debug!("Hello sent");

    if let Some(ClientMessage::Hello(_name)) = recv.try_next().await? {
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

            send.send(ServerMessage::Request(job)).await?;
        }

        debug!("Done!");
    }

    while let Some(message) = recv.try_next().await? {
        debug!("Message:{:?}", message);
    }

    Ok(())
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
