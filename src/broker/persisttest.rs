use async_trait::async_trait;

use super::router::SessionId;
use crate::broker::persist::PersistProvider;
use crate::broker::persist::PersistedTopic;
use crate::broker::BrokerError;
use crate::broker::BrokerId;

use crate::messages::MQTTMessagePublish;

pub struct PersistTopicTest {
    broker_id: BrokerId,
}

impl PersistTopicTest {
    pub fn new(broker_id: BrokerId) -> PersistTopicTest {
        PersistTopicTest { broker_id }
    }
}

#[async_trait]
impl PersistProvider for PersistTopicTest {
    async fn create_persistent_connection(&mut self) -> Result<u8, BrokerError> {
        Ok(0)
    }

    async fn init_persistent_store(&mut self) -> Result<u8, BrokerError> {
        Ok(0)
    }

    async fn load_persistent_topics(&mut self) -> Result<Vec<PersistedTopic>, BrokerError> {
        Ok(Vec::new())
    }

    async fn load_persistent_message(
        &mut self,
        _message_id: i32,
    ) -> Result<MQTTMessagePublish, BrokerError> {
        Err(BrokerError::PersistError("Persist test error".to_owned()))
    }

    async fn load_persistent_subscriptions(
        &mut self,
        _topic_id: i32,
    ) -> Result<Vec<(SessionId, u8)>, BrokerError> {
        let result: Vec<(SessionId, u8)> = Vec::new();

        return Ok(result);
    }

    async fn persist_topic(&mut self, _topic: String) -> Result<i32, BrokerError> {
        let value = 0_i32;
        Ok(value)
    }

    async fn persist_delete_topic(&mut self, topic_id: i32) -> Result<i32, BrokerError> {
        Ok(topic_id)
    }

    async fn persist_subscribe(
        &mut self,
        _session_id: i32,
        _topic_id: i32,
        _qos: u8,
    ) -> Result<bool, BrokerError> {
        Ok(true)
    }

    async fn persist_update_subscribe(
        &mut self,
        _session_id: i32,
        _topic_id: i32,
        _qos: u8,
    ) -> Result<bool, BrokerError> {
        Ok(true)
    }

    async fn persist_retain(
        &mut self,
        _topic_id: i32,
        _msg: &MQTTMessagePublish,
    ) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn persist_delete_retain(&mut self, _topic_id: i32) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn persist_unsubscribe(
        &mut self,
        _session_id: i32,
        _topic_id: i32,
    ) -> Result<bool, BrokerError> {
        Ok(true)
    }
}
