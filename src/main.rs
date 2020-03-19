use anyhow::Error;
use log::*;
use serde_json::Value;
use std::env;
use std::sync::Arc;
use structopt::StructOpt;
use tmq::{router, dealer, Context, SocketExt, Multipart};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
use futures::{future, SinkExt, TryStreamExt, StreamExt};

use tokio::time::delay_for;
use tokio::runtime;
use std::time::Duration;


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
}

async fn server(config: ConfigContext) -> Result<(), Error> {
    debug!("Connecting to db:{}", config.connect_url);
    let handle = DbHandle::new(&config.connect_url).await?;

    let (mut recv, mut send) = router(&Context::new()).bind(&config.job_address)?.split();

    while let Some(msg) = recv.try_next().await? {
        debug!("Message received:{:?}", msg);
    }


    Ok(())
}

async fn worker(config: ConfigContext, name: &str) -> Result<(), Error> {

    let mut socket = dealer(&Context::new()).connect(&config.job_address)?;

    socket.set_identity(name.as_bytes())?;

    loop {
        delay_for(Duration::from_secs(1)).await;

        socket.send(vec!("Hello".as_bytes())).await?;
    }

}

fn setup() -> Result<(), Error> {

    let config = ConfigContext::from_args();

    let server = server(config.clone());
    let worker = worker(config.clone(), "test");

    let mut runtime = runtime::Builder::new().threaded_scheduler().enable_all().build()?;


    let server_task = tokio::task::LocalSet::new();

    let worker_task = tokio::task::LocalSet::new();


    runtime.block_on(async {
        future::try_join(server_task.run_until(server), worker_task.run_until(worker)).await
    })?;


    Ok(())
}

fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "jobq=DEBUG");
    }

    pretty_env_logger::init_timed();

    if let Err(err) = setup() {
        error!("{}", err);
    }

}

#[derive(Clone)]
struct DbHandle {
    client: Arc<Client>,
}

impl DbHandle {
    async fn new(url: &str) -> Result<Self, Error> {
        let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        client.batch_execute(include_str!("setup.sql")).await?;

        Ok(DbHandle {
            client: Arc::new(client),
        })
    }

    async fn submit_job(&self, job: &Job) -> Result<i32, Error> {
        let query =
            "INSERT into jobq (name, uuid, params, priority) values ($1, $2, $3, $4) returning id";

        let result = self
            .client
            .query(query, &[&job.name, &job.uuid, &job.params, &job.priority])
            .await?;

        Ok(result[0].get(0))
    }
}

struct Job {
    name: String,
    uuid: Uuid,
    params: Value,
    priority: i32,
}

enum Status {
    Queued,
    Processing,
    Completed,
    Failed,
}
