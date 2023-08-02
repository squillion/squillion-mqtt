use async_trait::async_trait;
use std::collections::HashSet;

use crate::broker::BrokerId;
use crate::messages::codec::MQTTMessage;
use crate::sessions::persist::SessionPersistProvider;
use crate::sessions::SessionError;

pub struct PersistSessionTest {}

impl PersistSessionTest {
    pub fn new(_broker_id: BrokerId) -> PersistSessionTest {
        PersistSessionTest {}
    }
}

#[async_trait]
impl SessionPersistProvider for PersistSessionTest {
    async fn create_persistent_connection(&mut self) -> Result<u8, SessionError> {
        Ok(0)
    }

    async fn persist_session(
        &mut self,
        _id: String,
    ) -> Result<(i32, i64, bool, u16), SessionError> {
        let generation = 1_i64;
        let internal_id = 1_i32;
        let clean_session = true;
        let next_mid = 1_u16;

        Ok((internal_id, generation, clean_session, next_mid))
    }

    async fn load_persistent_subscriptions(
        &mut self,
        _session_id: i32,
    ) -> Result<Vec<String>, SessionError> {
        let result: Vec<String> = Vec::new();

        return Ok(result);
    }

    async fn persist_update_session_generation(
        &mut self,
        internal_id: i32,
        _generation: i64,
    ) -> Result<i32, SessionError> {
        Ok(internal_id)
    }

    async fn persist_qos_incoming_store(
        &mut self,
        _session_id: i32,
        _mid: u16,
    ) -> Result<i32, SessionError> {
        Ok(0)
    }

    async fn persist_qos_incoming_delete(
        &mut self,
        _session_id: i32,
        _mid: u16,
    ) -> Result<u16, SessionError> {
        Ok(0)
    }

    async fn persist_qos_incoming_clear(&mut self, _session_id: i32) -> Result<i32, SessionError> {
        Ok(0)
    }

    async fn persist_qos_incoming_load(
        &mut self,
        _session_id: i32,
    ) -> Result<HashSet<u16>, SessionError> {
        let mids: HashSet<u16> = HashSet::new();
        Ok(mids)
    }

    async fn persist_qos_outgoing_store(
        &mut self,
        _session_id: i32,
        _mid: u16,
        _msg: &MQTTMessage,
    ) -> Result<u16, SessionError> {
        Ok(0)
    }

    async fn persist_qos_outgoing_delete(
        &mut self,
        _session_id: i32,
        _mid: u16,
    ) -> Result<u16, SessionError> {
        Ok(0)
    }

    async fn persist_qos_outgoing_update_dup(
        &mut self,
        _session_id: i32,
        _mid: u16,
        _dup: bool,
    ) -> Result<u16, SessionError> {
        Ok(0)
    }

    async fn persist_qos_outgoing_clear(&mut self, _session_id: i32) -> Result<u16, SessionError> {
        Ok(0)
    }

    async fn persist_update_session_mid(
        &mut self,
        _internal_id: i32,
        _mid: u16,
    ) -> Result<u16, SessionError> {
        Ok(0)
    }

    async fn persistent_qosout_load(
        &mut self,
        _session_id: i32,
    ) -> Result<Vec<MQTTMessage>, SessionError> {
        let messages = Vec::new();
        Ok(messages)
    }
}
