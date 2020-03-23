use crate::{Job, JobRequest};
use anyhow::Error;
use log::*;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls, Row};

#[derive(Clone)]
pub struct DbHandle {
    client: Arc<Client>,
}

impl DbHandle {
    pub(crate) async fn new(url: &str) -> Result<Self, Error> {
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

    pub(crate) async fn complete_job(&self, id: i64) -> Result<(), Error> {
        let query = "update jobq set status = 'Completed', duration = extract(epoch from now() - \"time\") where id = $1";

        self.client.query(query, &[&id]).await?;

        Ok(())
    }

    pub(crate) async fn fail_job(&self, id: i64, msg: String) -> Result<(), Error> {
        let query = "update jobq set status = 'Failed', duration = extract(epoch from now() - \"time\"), error = $1 where id = $2";

        self.client.query(query, &[&msg, &id]).await?;

        Ok(())
    }

    pub(crate) async fn begin_job(&self, id: i64) -> Result<(), Error> {
        let query = "update jobq set status = 'Processing', time = now() where id = $1";

        self.client.query(query, &[&id]).await?;

        Ok(())
    }

    fn get_jobs(result: Vec<Row>) -> Result<Vec<Job>, Error> {
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

    pub(crate) async fn get_processing_jobs(&self) -> Result<Vec<Job>, Error> {
        let query = "select id, name, uuid, params, priority, status from jobq where status = 'Processing' order by priority asc, time asc";

        DbHandle::get_jobs(self.client.query(query, &[]).await?)
    }

    pub(crate) async fn get_queued_jobs(&self, num: i64) -> Result<Vec<Job>, Error> {
        let query = "select id, name, uuid, params, priority, status from jobq where status = 'Queued' order by priority asc, time asc limit $1";

        DbHandle::get_jobs(self.client.query(query, &[&num]).await?)
    }

    pub(crate) async fn submit_job_request(&self, job: &JobRequest) -> Result<i64, Error> {
        let query =
            "INSERT into jobq (name, uuid, params, priority, status) values ($1, $2, $3, $4, 'Queued') returning id";

        let result = self
            .client
            .query(query, &[&job.name, &job.uuid, &job.params, &job.priority])
            .await?;

        Ok(result[0].get(0))
    }
}
