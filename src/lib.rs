use anyhow::Error;
use postgres_types::{FromSql, ToSql};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use tmq::{Message, Multipart};
use uuid::Uuid;

pub mod db;
pub mod server;
pub mod worker;

#[derive(Serialize, Deserialize, Debug)]
pub enum JobqMessage {
    Hello,
    Request(JobRequest),
    Order(Job),
    Completed(Job),
    Failed(Job, String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobRequest {
    pub name: String,
    pub uuid: Uuid,
    pub params: Value,
    pub priority: Priority,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Job {
    pub id: i64,
    pub name: String,
    pub uuid: Uuid,
    pub params: Value,
    pub priority: Priority,
    pub status: Status,
}

#[derive(Serialize, Deserialize, Debug, ToSql, FromSql)]
pub enum Status {
    Queued,
    Processing,
    Completed,
    Failed,
}

#[derive(Serialize, Deserialize, Debug, ToSql, FromSql)]
pub enum Priority {
    High,
    Normal,
    Low,
}

impl JobqMessage {
    pub fn to_mpart(self) -> Result<Multipart, Error> {
        let bytes = serde_cbor::to_vec(&self)?;

        Ok(Multipart::from(vec![&bytes]))
    }
    pub fn to_msg(self) -> Result<Message, Error> {
        let bytes = serde_cbor::to_vec(&self)?;

        Ok(Message::from(&bytes))
    }
}
