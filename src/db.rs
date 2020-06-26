use crate::{Job, JobRequest};
use anyhow::Error;
use sqlx::postgres::{PgPool, PgQueryAs};
use sqlx::Executor;
use std::sync::Arc;

#[derive(Clone)]
pub struct DbHandle {
    pool: Arc<PgPool>,
}

impl DbHandle {
    pub(crate) async fn new(url: &str) -> Result<Self, Error> {
        let pool = PgPool::builder()
            .max_size(5) // maximum number of connections in the pool
            .build(&url)
            .await?;

        (&pool).execute(include_str!("setup.sql")).await?;

        Ok(DbHandle {
            pool: Arc::new(pool),
        })
    }

    pub(crate) async fn complete_job(&self, id: i64) -> Result<(), Error> {
        let query = "update jobq set status = 'Completed', duration = extract(epoch from now() - \"time\") where id = $1";

        sqlx::query(query).bind(&id).execute(&*self.pool).await?;

        Ok(())
    }

    pub(crate) async fn fail_job(&self, id: i64, msg: String) -> Result<(), Error> {
        let query = "update jobq set status = 'Failed', duration = extract(epoch from now() - \"time\"), error = $1 where id = $2";

        sqlx::query(query)
            .bind(&msg)
            .bind(&id)
            .execute(&*self.pool)
            .await?;

        Ok(())
    }

    pub(crate) async fn begin_job(&self, id: i64) -> Result<(), Error> {
        let query = "update jobq set status = 'Processing', time = now() where id = $1";

        sqlx::query(query).bind(&id).execute(&*self.pool).await?;

        Ok(())
    }

    pub(crate) async fn get_processing_jobs(&self) -> Result<Vec<Job>, Error> {
        let query = "select id, name, username, uuid, params, priority, status from jobq
                     where status = 'Processing' order by priority asc, time asc";

        Ok(sqlx::query_as(query).fetch_all(&*self.pool).await?)
    }

    pub(crate) async fn get_queued_jobs(&self, num: i64) -> Result<Vec<Job>, Error> {
        let query = "select 
                        id,
                        name,
                        username,
                        uuid,
                        params,
                        priority,
                        status
                     from jobq
                     where 
                        status = 'Queued'
                     order by
                     priority asc, time asc
                     limit $1";

        Ok(sqlx::query_as(query)
            .bind(&num)
            .fetch_all(&*self.pool)
            .await?)
    }

    pub(crate) async fn submit_job_request(&self, job: &JobRequest) -> Result<i64, Error> {
        let query =
            "INSERT into jobq (name, username, uuid, params, priority, status) values ($1, $2, $3, $4, $5, 'Queued') returning id";

        let result: (i64,) = sqlx::query_as(query)
            .bind(&job.name)
            .bind(&job.username)
            .bind(&job.uuid)
            .bind(&job.params)
            .bind(&job.priority)
            .fetch_one(&*self.pool)
            .await?;

        Ok(result.0)
    }
}
