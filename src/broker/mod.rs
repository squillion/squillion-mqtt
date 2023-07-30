pub mod persist;
pub mod persistsql;
pub mod persisttest;
pub mod router;
pub mod topics;

use std::cmp::Eq;
use std::error;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;

use futures::channel::mpsc;
use futures::channel::oneshot;

use futures::stream::StreamExt;

use crate::broker::persist::get_persist_pool;
use deadpool_sqlite::Pool;

use crate::broker::router::MessageRouter;
use crate::broker::router::MessageRouterTx;
use crate::sessions::sessionworker::SessionTx;
use crate::sessions::Sessions;

#[cfg(test)]
mod testpersist;

#[derive(Debug, PartialEq, Eq)]
pub enum BrokerError {
    PersistError(String),
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BrokerError::PersistError(ref err) => write!(f, "BrokerError::PersistError: {}", err),
        }
    }
}

impl error::Error for BrokerError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            BrokerError::PersistError(_) => None,
        }
    }
}

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Default, Clone)]
pub struct BrokerId {
    pub tenant_id: String,
    pub broker_id: String,
}

impl fmt::Display for BrokerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tenant {} Broker {}", self.tenant_id, self.broker_id)
    }
}

impl BrokerId {
    pub fn test_broker() -> BrokerId {
        BrokerId {
            tenant_id: "test_tenant".to_string(),
            broker_id: "test_broker".to_string(),
        }
    }
}

pub struct RouterState {
    pub running: bool,
}

impl RouterState {
    pub fn new() -> RouterState {
        RouterState { running: false }
    }
}

type TenantState = Arc<Mutex<Shared>>;

pub struct Shared {
    pub sessions: Sessions,
}

impl Shared {
    pub fn new(logger: slog::Logger, broker_id: BrokerId) -> Shared {
        Shared {
            sessions: Sessions::new(logger, broker_id),
        }
    }
}

pub struct SessionResponse {
    pub session: Option<SessionTx>,
}

// Shorthand for the transmit half of the message channel.
pub type SessionRequestTx = oneshot::Sender<SessionResponse>;
pub type SessionRequestRx = oneshot::Receiver<SessionResponse>;

pub struct SessionRequest {
    pub response_channel: SessionRequestTx,
    pub session: String,
}

impl SessionRequest {
    pub fn new(session: String, response_channel: SessionRequestTx) -> SessionRequest {
        SessionRequest {
            response_channel,
            session,
        }
    }
}

pub enum BrokerMessage {
    GetSessionTx(SessionRequest),
}

/// Shorthand for the transmit half of the message channel.
pub type BrokerTx = futures::channel::mpsc::UnboundedSender<BrokerMessage>;

/// Shorthand for the receive half of the message channel.
type BrokerRx = futures::channel::mpsc::UnboundedReceiver<BrokerMessage>;

pub struct MqttBroker {
    logger: slog::Logger,
    broker_id: BrokerId,
    router_tx: Option<MessageRouterTx>,
    router_state: Arc<Mutex<RouterState>>,
    state: TenantState,
    broker_rx: BrokerRx,
    broker_tx: BrokerTx,
    database_pool: Option<Pool>,
}

impl MqttBroker {
    pub fn new(
        logger: slog::Logger,
        broker_id: BrokerId,
        database_pool: Option<Pool>,
    ) -> MqttBroker {
        let logger = logger.new(slog::o!("tenant" => broker_id.tenant_id.clone(),
        "broker" => broker_id.broker_id.clone()));

        let channels = Arc::new(Mutex::new(Shared::new(logger.clone(), broker_id.clone())));

        let (broker_tx, broker_rx): (
            mpsc::UnboundedSender<BrokerMessage>,
            mpsc::UnboundedReceiver<BrokerMessage>,
        ) = mpsc::unbounded();

        MqttBroker {
            logger,
            broker_id,
            router_tx: None,
            router_state: Arc::new(Mutex::new(RouterState::new())),
            state: channels,
            broker_rx,
            broker_tx,
            database_pool,
        }
    }

    pub fn get_tx(&mut self) -> &BrokerTx {
        &self.broker_tx
    }

    pub async fn handle_session_request(&mut self, request: SessionRequest) {
        let mut sessiontx = None;

        if !self.router_state.lock().unwrap().running {
            self.start_router().await;
        }

        if let Some(message_router_tx) = &self.router_tx {
            let session = self.state.lock().unwrap().sessions.get_session(
                request.session.clone(),
                message_router_tx.clone(),
                self.database_pool.clone(),
            );

            match session {
                Ok(session_tx) => {
                    sessiontx = Some(session_tx);
                }
                Err(_error) => {
                    slog::error!(self.logger, "Unable to get session {}", request.session);
                }
            };
        } else {
            slog::warn!(self.logger, "Message router not running!");
        }

        let session_response = SessionResponse { session: sessiontx };
        if request.response_channel.send(session_response).is_err() {
            slog::warn!(self.logger, "Unable to send session response");
        }
    }

    pub async fn start_router(&mut self) {
        if self.database_pool.is_none() {
            self.database_pool = Some(get_persist_pool(&self.broker_id));
        }
        let mut message_router = MessageRouter::new(
            self.logger.clone(),
            self.broker_id.clone(),
            self.router_state.clone(),
            self.state.clone(),
            self.database_pool.clone(),
        );
        self.router_tx = Some(message_router.get_tx().clone());
        match message_router.init_persistent_store().await {
            Err(err) => {
                slog::warn!(self.logger, "Unable to init persistent store: {}", err);
                self.router_tx = None;
            }
            Ok(_) => {
                self.router_state.lock().unwrap().running = true;
                tokio::spawn(async move {
                    message_router.start().await;
                });
            }
        }
    }

    pub async fn run_loop(&mut self) {
        self.start_router().await;

        loop {
            let broker_event = self.broker_rx.next().await;
            if let Some(v) = broker_event {
                match v {
                    BrokerMessage::GetSessionTx(request) => {
                        self.handle_session_request(request).await;
                    }
                };
            }
        }
    }
}
