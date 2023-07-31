use std::fs;
use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;

use super::router::SessionId;
use crate::broker::persistsql::PersistTopicSQL;
use crate::broker::persisttest::PersistTopicTest;
use crate::broker::BrokerError;
use crate::broker::BrokerId;
use crate::config;
use crate::messages::MQTTMessagePublish;
use deadpool_sqlite::Pool;

pub struct PersistedTopic {
    pub topic_id: i32,
    pub topic_name: String,
    pub retained_message_id: Option<i32>,
}

#[async_trait]
pub trait PersistProvider: Sync + Send {
    async fn create_persistent_connection(&mut self) -> Result<u8, BrokerError>;

    async fn init_persistent_store(&mut self) -> Result<u8, BrokerError>;

    async fn load_persistent_topics(&mut self) -> Result<Vec<PersistedTopic>, BrokerError>;

    async fn load_persistent_message(
        &mut self,
        message_id: i32,
    ) -> Result<MQTTMessagePublish, BrokerError>;

    async fn load_persistent_subscriptions(
        &mut self,
        topic_id: i32,
    ) -> Result<Vec<(SessionId, u8)>, BrokerError>;

    async fn persist_topic(&mut self, topic: String) -> Result<i32, BrokerError>;

    async fn persist_delete_topic(&mut self, topic_id: i32) -> Result<i32, BrokerError>;

    async fn persist_subscribe(
        &mut self,
        session_id: i32,
        topic_id: i32,
        qos: u8,
    ) -> Result<bool, BrokerError>;

    async fn persist_update_subscribe(
        &mut self,
        session_id: i32,
        topic_id: i32,
        qos: u8,
    ) -> Result<bool, BrokerError>;

    async fn persist_retain(
        &mut self,
        topic_id: i32,
        msg: &MQTTMessagePublish,
    ) -> Result<(), BrokerError>;

    async fn persist_delete_retain(&mut self, topic_id: i32) -> Result<(), BrokerError>;

    async fn persist_unsubscribe(
        &mut self,
        session_id: i32,
        topic_id: i32,
    ) -> Result<bool, BrokerError>;
}

pub fn get_persist_provider(
    broker_id: BrokerId,
    database_pool: Option<Pool>,
) -> Box<dyn PersistProvider> {
    let persist_method = config::get_string("persist_method").unwrap();

    match persist_method.as_str() {
        "postgres" => Box::new(PersistTopicSQL::new(broker_id, database_pool.unwrap())),
        "sqlite" => Box::new(PersistTopicSQL::new(broker_id, database_pool.unwrap())),
        "test" => Box::new(PersistTopicTest::new(broker_id)),
        _ => Box::new(PersistTopicSQL::new(broker_id, database_pool.unwrap())),
    }
}

pub fn get_persist_pool(broker_id: &BrokerId) -> Pool {
    let persist_path = config::get_string("persist_data_store").unwrap();
    let mut path: PathBuf = Path::new(&persist_path).to_path_buf();

    if let Some(tenant_id) = &broker_id.tenant_id {
        path = path.join("tenants").join(tenant_id);
    } else {
        path = path.join("none");
    }

    fs::create_dir_all(&path).unwrap();

    path = path.join("database.sqlite3");

    let cfg = deadpool_sqlite::Config::new(path);
    cfg.create_pool(deadpool_sqlite::Runtime::Tokio1).unwrap()
}
