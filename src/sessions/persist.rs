use async_trait::async_trait;
use deadpool_sqlite::Pool;

use std::collections::HashSet;

use crate::broker::BrokerId;
use crate::config;
use crate::messages::codec::MQTTMessage;
use crate::sessions::persistsql::PersistSessionSQL;
use crate::sessions::persisttest::PersistSessionTest;
use crate::sessions::SessionError;

#[async_trait]
pub trait SessionPersistProvider: Sync + Send {
    async fn create_persistent_connection(&mut self) -> Result<u8, SessionError>;

    async fn persist_session(&mut self, id: String) -> Result<(i32, i64, bool, u16), SessionError>;

    async fn persist_update_session(
        &mut self,
        internal_id: i32,
        generation: i64,
    ) -> Result<i32, SessionError>;

    async fn load_persistent_subscriptions(
        &mut self,
        session_id: i32,
    ) -> Result<Vec<String>, SessionError>;

    async fn persist_qos_incoming_store(
        &mut self,
        session_id: i32,
        mid: u16,
    ) -> Result<i32, SessionError>;

    async fn persist_qos_incoming_delete(
        &mut self,
        session_id: i32,
        mid: u16,
    ) -> Result<u16, SessionError>;

    async fn persist_qos_incoming_clear(&mut self, session_id: i32) -> Result<i32, SessionError>;

    async fn persist_qos_incoming_load(
        &mut self,
        session_id: i32,
    ) -> Result<HashSet<u16>, SessionError>;

    async fn persist_qos_outgoing_store(
        &mut self,
        session_id: i32,
        mid: u16,
        msg: &MQTTMessage,
    ) -> Result<u16, SessionError>;

    async fn persist_qos_outgoing_delete(
        &mut self,
        session_id: i32,
        mid: u16,
    ) -> Result<u16, SessionError>;

    async fn persist_qos_outgoing_update_dup(
        &mut self,
        session_id: i32,
        mid: u16,
        dup: bool,
    ) -> Result<u16, SessionError>;

    async fn persist_qos_outgoing_clear(&mut self, session_id: i32) -> Result<u16, SessionError>;

    async fn persist_update_session_mid(
        &mut self,
        internal_id: i32,
        mid: u16,
    ) -> Result<u16, SessionError>;

    async fn persistent_qosout_load(
        &mut self,
        session_id: i32,
    ) -> Result<Vec<MQTTMessage>, SessionError>;
}

pub fn get_session_persist_provider(
    broker_id: BrokerId,
    database_pool: Option<Pool>,
) -> Box<dyn SessionPersistProvider> {
    let persist_method = config::get_string("persist_method").unwrap();

    match persist_method.as_str() {
        "sqlite" => Box::new(PersistSessionSQL::new(broker_id, database_pool.unwrap())),
        "test" => Box::new(PersistSessionTest::new(broker_id)),
        _ => Box::new(PersistSessionSQL::new(broker_id, database_pool.unwrap())),
    }
}
