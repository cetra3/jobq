use anyhow::{anyhow, Error};
use futures::{SinkExt, StreamExt, TryStream, TryStreamExt};
use log::*;
use postgres_types::{FromSql, ToSql};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::sync::Arc;
use structopt::StructOpt;
use tmq::{dealer, Context, Message, Multipart, TmqError};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

use std::time::Duration;
use tokio::time::delay_for;

mod server;
mod worker;

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

#[derive(Serialize, Deserialize, Debug)]
enum JobqMessage {
    Hello,
    Request(JobRequest),
    Order(Job),
    Completed(Job),
    Failed(Job, String),
}

#[derive(Serialize, Deserialize, Debug)]
struct JobRequest {
    name: String,
    uuid: Uuid,
    params: Value,
    priority: Priority,
}

#[derive(Serialize, Deserialize, Debug)]
struct Job {
    id: i64,
    name: String,
    uuid: Uuid,
    params: Value,
    priority: Priority,
    status: Status,
}

#[derive(Serialize, Deserialize, Debug, ToSql, FromSql)]
enum Status {
    Queued,
    Processing,
    Completed,
    Failed,
}

#[derive(Serialize, Deserialize, Debug, ToSql, FromSql)]
enum Priority {
    High,
    Normal,
    Low,
}

impl JobqMessage {
    fn to_mpart(self) -> Result<Multipart, Error> {
        let bytes = serde_cbor::to_vec(&self)?;

        Ok(Multipart::from(vec![&bytes]))
    }
    fn to_msg(self) -> Result<Message, Error> {
        let bytes = serde_cbor::to_vec(&self)?;

        Ok(Message::from(&bytes))
    }
}

async fn get_message<S: TryStream<Ok = Multipart, Error = TmqError> + Unpin>(
    recv: &mut S,
) -> Result<JobqMessage, Error> {
    if let Some(msg) = recv.try_next().await? {
        let jobq_message: JobqMessage = serde_cbor::from_slice(&msg[0])?;

        Ok(jobq_message)
    } else {
        Err(anyhow!("No Messages in Stream"))
    }
}

async fn setup() -> Result<(), Error> {
    let config = ConfigContext::from_args();

    let server = server::server(config.clone());

    let worker = worker::worker(config.clone(), "test");

    tokio::spawn(async {
        if let Err(err) = server.await {
            error!("{}", err);
        }
    });

    delay_for(Duration::from_secs(1)).await;

    tokio::spawn(async {
        if let Err(err) = worker.await {
            error!("{}", err);
        }
    });

    let (mut send, mut recv) = dealer(&Context::new())
        .set_identity(b"test_client")
        .connect(&config.job_address)?
        .split::<Multipart>();

    //Send hello
    send.send(JobqMessage::Hello.to_mpart()?).await?;

    if let JobqMessage::Hello = get_message(&mut recv).await? {
        debug!("Received Hello response, sending a couple of jobs");

        for _ in 0..5000 {
            let job = JobRequest {
                name: "test".into(),
                params: Value::Null,
                uuid: Uuid::new_v4(),
                priority: Priority::Normal,
            };

            send.send(JobqMessage::Request(job).to_mpart()?).await?;
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

    async fn complete_job(&self, id: i64) -> Result<(), Error> {
        let query = "update jobq set status = 'Completed', duration = extract(epoch from now() - \"time\") where id = $1";

        self.client.query(query, &[&id]).await?;

        Ok(())
    }

    async fn fail_job(&self, id: i64, msg: String) -> Result<(), Error> {
        let query = "update jobq set status = 'Failed', duration = extract(epoch from now() - \"time\"), error = $1 where id = $2";

        self.client.query(query, &[&msg, &id]).await?;

        Ok(())
    }

    async fn begin_job(&self, id: i64) -> Result<(), Error> {
        let query = "update jobq set status = 'Processing', time = now() where id = $1";

        self.client.query(query, &[&id]).await?;

        Ok(())
    }

    async fn get_queued_jobs(&self, num: i64) -> Result<Vec<Job>, Error> {
        let query = "select id, name, uuid, params, priority, status from jobq where status = 'Queued' order by priority asc, time asc limit $1";

        let result = self.client.query(query, &[&num]).await?;

        let mut jobs = Vec::new();

        for row in result {
            let id = row.get(0);
            let name = row.get(1);
            let uuid = row.get(2);
            let params = row.get(3);
            let priority = row.get(4);
            let status = row.get(5);

            jobs.push({
                Job {
                    id,
                    name,
                    uuid,
                    params,
                    priority,
                    status,
                }
            });
        }

        Ok(jobs)
    }

    async fn get_processing(&self) -> Result<i64, Error> {
        let query = "select count(*) from jobq where status = 'Processing'";

        let result = self.client.query(query, &[]).await?;

        Ok(result[0].get(0))
    }

    async fn get_queued(&self) -> Result<i64, Error> {
        let query = "select count(*) from jobq where status = 'Queued'";

        let result = self.client.query(query, &[]).await?;

        Ok(result[0].get(0))
    }

    async fn submit_job_request(&self, job: &JobRequest) -> Result<i64, Error> {
        let query =
            "INSERT into jobq (name, uuid, params, priority, status) values ($1, $2, $3, $4, 'Queued') returning id";

        let result = self
            .client
            .query(query, &[&job.name, &job.uuid, &job.params, &job.priority])
            .await?;

        Ok(result[0].get(0))
    }
}
