use futures::channel::mpsc;
use futures::channel::oneshot;
use std::sync::Arc;
use std::sync::Mutex;

use futures::stream::StreamExt;

use deadpool_sqlite::Pool;

use crate::broker::BrokerError;
use crate::broker::RouterState;
use crate::broker::Shared;
use crate::messages::MQTTMessagePublish;

use crate::messages::codec::MQTTMessage;

use super::topics::Subscriptions;
use crate::sessions::sessionworker::SessionInternalMessage;
use crate::sessions::sessionworker::SessionMessage;
use crate::BrokerId;

type TenantState = Arc<Mutex<Shared>>;

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct SessionId {
    pub session: String,
    pub internal_id: i32,
    pub session_generation: i64,
}

pub struct RouterPublishSubscribeResponse {
    pub session: SessionId,
    pub ret: u8,
    pub retained_messages: Vec<MQTTMessagePublish>,
}

pub type RouterSubResponseTx = oneshot::Sender<RouterPublishSubscribeResponse>;
pub type RouterSubResponseRx = oneshot::Receiver<RouterPublishSubscribeResponse>;

pub struct RouterPublishMessage {
    pub session: SessionId,
    pub message: MQTTMessagePublish,
}

pub struct RouterPublishSubscribe {
    pub session: SessionId,
    pub topic: String,
    pub qos: u8,
    pub response_tx: RouterSubResponseTx,
}

pub struct RouterPublisUnsubscribe {
    pub session: SessionId,
    pub topics: Vec<String>,
}

pub enum RouterMessage {
    Publish(RouterPublishMessage),
    Subscribe(RouterPublishSubscribe),
    Unsubscribe(RouterPublisUnsubscribe),
}

pub type MessageRouterTx = mpsc::UnboundedSender<RouterMessage>;
pub type MessageRouterRx = mpsc::UnboundedReceiver<RouterMessage>;

pub struct MessageRouter {
    logger: slog::Logger,
    topics: Subscriptions,
    router_tx: MessageRouterTx,
    router_rx: MessageRouterRx,
    router_state: Arc<Mutex<RouterState>>,
    state: TenantState,
    database_pool: Option<Pool>,
}

impl MessageRouter {
    pub fn new(
        logger: slog::Logger,
        broker_id: BrokerId,
        router_state: Arc<Mutex<RouterState>>,
        tenant_state: TenantState,
        database_pool: Option<Pool>,
    ) -> Self {
        let (router_tx, router_rx): (MessageRouterTx, MessageRouterRx) = mpsc::unbounded();

        MessageRouter {
            logger: logger.clone(),
            topics: Subscriptions::new(broker_id, logger.clone(), database_pool.clone()),
            router_tx,
            router_rx,
            router_state,
            state: tenant_state,
            database_pool,
        }
    }

    pub fn get_tx(&self) -> &MessageRouterTx {
        &self.router_tx
    }

    pub async fn publish_message(&mut self, msg: MQTTMessagePublish) -> Result<(), BrokerError> {
        slog::debug!(self.logger, "Forwarding to topic {}", msg.get_topic());
        if msg.get_retain() {
            self.topics.retain(msg.clone()).await?;
        }

        let subscribed = self.topics.get_subscribed(msg.get_topic());

        slog::debug!(
            self.logger,
            "Forwarding to topic {} num. subscribed {}",
            msg.get_topic(),
            subscribed.len()
        );
        for (session, qos) in subscribed {
            let mut session_tx = {
                let shared_state = self.state.lock().unwrap();
                shared_state
                    .sessions
                    .get_session_tx(&session.session)
                    .cloned()
            };

            if session_tx.is_none() && qos > 0 {
                // TODO attempt to start session
                // TODO tmp hack until handling peristence nicely
                let mut shared = self.state.lock().unwrap();
                let stx = shared.sessions.get_session(
                    session.session.clone(),
                    self.router_tx.clone(),
                    self.database_pool.clone(),
                );
                if let Ok(tx) = stx {
                    session_tx = Some(tx);
                }
            }

            if let Some(tx) = session_tx {
                let mut cmsg = msg.clone();
                if cmsg.get_qos() > qos {
                    cmsg.set_qos(qos);
                }
                let msg = SessionInternalMessage {
                    session,
                    message: MQTTMessage::Publish(cmsg),
                };
                if tx.unbounded_send(SessionMessage::Internal(msg)).is_err() {
                    // TODO: Handle QoS messages here?
                    // Should probably be a persistent queue
                    slog::warn!(self.logger, "Error sending message to session");
                }
            } else {
                slog::debug!(self.logger, "Message send - session not found");
            }
        }

        Ok(())
    }

    pub async fn subscribe_message(
        &mut self,
        msg: RouterPublishSubscribe,
    ) -> Result<(), BrokerError> {
        let mut response = RouterPublishSubscribeResponse {
            session: msg.session.clone(),
            ret: 0x80, /* Error */
            retained_messages: Vec::new(),
        };

        let qos = self
            .topics
            .subscribe(msg.session, &msg.topic, msg.qos)
            .await?;

        if qos == 0x80 {
            response.ret = 0x80;
        } else {
            let mut retained_messages = self.topics.get_retained_messages(msg.topic);
            for cmsg in &mut retained_messages {
                if cmsg.get_qos() > qos {
                    cmsg.set_qos(qos);
                }
            }
            response.retained_messages.append(&mut retained_messages);
            response.ret = qos;
        }

        if msg.response_tx.send(response).is_err() {
            slog::warn!(
                self.logger,
                "Failed sending subscription response to session"
            );
        }

        Ok(())
    }

    pub async fn unsubscribe_message(
        &mut self,
        msg: RouterPublisUnsubscribe,
    ) -> Result<(), BrokerError> {
        for topic in msg.topics.iter() {
            self.topics.unsubscribe(&msg.session, topic).await?;
        }

        Ok(())
    }

    pub async fn init_persistent_store(&mut self) -> Result<u8, BrokerError> {
        self.topics.create_persistent_connection().await?;
        self.topics.init_persistent_store().await?;
        self.topics.load_persistent_state().await?;

        Ok(0)
    }

    pub async fn start(&mut self) {
        let mut running: bool = true;

        while running {
            let event = self.router_rx.next().await;
            let err = match event {
                Some(message) => match message {
                    RouterMessage::Publish(msg) => self.publish_message(msg.message).await,
                    RouterMessage::Subscribe(msg) => self.subscribe_message(msg).await,
                    RouterMessage::Unsubscribe(msg) => self.unsubscribe_message(msg).await,
                },
                None => Ok(()),
            };

            if let Err(e) = err {
                match e {
                    BrokerError::PersistError(_) => {
                        slog::error!(self.logger, "Router stopping: {}", e);
                        running = false;
                    }
                }
            }
        }
    }
}

impl Drop for MessageRouter {
    fn drop(&mut self) {
        slog::debug!(self.logger, "Router Stopped");

        let mut ss = self.router_state.lock().unwrap();
        ss.running = false;
    }
}
