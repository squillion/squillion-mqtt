pub mod persist;
pub mod persistsql;
pub mod persisttest;
pub mod sessionworker;

use deadpool_sqlite::Pool;

use sessionworker::MQTTSession;
use sessionworker::SessionTx;
use std::collections::HashMap;

use std::error;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;

use crate::broker::BrokerId;

use crate::broker::router::MessageRouterTx;
use std::collections::hash_map::Entry::{Occupied, Vacant};

#[derive(Debug)]
pub enum SessionError {
    Client,
    Router,
    Persist(String),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SessionError::Client => write!(f, "cannot communicate with client"),
            SessionError::Router => write!(f, "canot communicate with router"),
            SessionError::Persist(ref s) => write!(f, "SessionError::Persist: {}", s),
        }
    }
}

impl error::Error for SessionError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            SessionError::Client => None,
            SessionError::Router => None,
            SessionError::Persist(_) => None,
        }
    }
}

pub struct SessionState {
    pub running: bool,
}

impl SessionState {
    pub fn new() -> SessionState {
        SessionState { running: true }
    }
}

pub struct Session {
    pub broker_id: BrokerId,
    pub id: String,
    pub session_tx: SessionTx,
    pub state: Arc<Mutex<SessionState>>,
}

impl Session {
    pub fn session_tx(&self) -> &SessionTx {
        &self.session_tx
    }
}

pub struct Sessions {
    logger: slog::Logger,
    broker_id: BrokerId,
    sessions: HashMap<String, Session>,
}

impl Sessions {
    pub fn new(logger: slog::Logger, broker_id: BrokerId) -> Sessions {
        Sessions {
            logger,
            broker_id,
            sessions: HashMap::new(),
        }
    }

    pub fn get_session_tx(&self, key: &str) -> Option<&SessionTx> {
        if let Some(session) = self.sessions.get(key) {
            if session.state.lock().unwrap().running {
                Some(session.session_tx())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn get_session(
        &mut self,
        id: String,
        message_router_tx: MessageRouterTx,
        database_pool: Option<Pool>,
    ) -> Result<SessionTx, &'static str> {
        match self.sessions.entry(id.clone()) {
            Vacant(entry) => {
                slog::debug!(self.logger, "New session {}", id);

                let session_state = Arc::new(Mutex::new(SessionState::new()));

                let session_tx = start_session(
                    self.logger.clone(),
                    self.broker_id.clone(),
                    id.clone(),
                    session_state.clone(),
                    message_router_tx,
                    database_pool,
                )?;

                let session = Session {
                    broker_id: self.broker_id.clone(),
                    id,
                    session_tx: session_tx.clone(),
                    state: session_state,
                };

                entry.insert(session);

                Ok(session_tx)
            }
            Occupied(entry) => {
                slog::debug!(self.logger, "Existing session {}", id);

                let e = entry.into_mut();
                if !e.state.lock().unwrap().running {
                    slog::debug!(self.logger, "Restarting session {}", id);

                    let session_tx = start_session(
                        self.logger.clone(),
                        self.broker_id.clone(),
                        id,
                        e.state.clone(),
                        message_router_tx,
                        database_pool,
                    )?;

                    e.session_tx = session_tx;
                }
                Ok(e.session_tx.clone())
            }
        }
    }
}

pub fn start_session(
    logger: slog::Logger,
    broker_id: BrokerId,
    id: String,
    session_state: Arc<Mutex<SessionState>>,
    message_router_tx: MessageRouterTx,
    database_pool: Option<Pool>,
) -> Result<SessionTx, &'static str> {
    let (session_tx, session_rx) = futures::channel::mpsc::unbounded();

    let ss = session_state.clone();
    let mut sslock = ss.lock().unwrap();
    sslock.running = true;

    let mut session_worker = MQTTSession::new(
        logger,
        broker_id,
        id,
        session_state,
        session_rx,        // session_rx
        message_router_tx, // message router
        database_pool,
    );

    tokio::spawn(async move {
        session_worker.run_loop().await;
    });

    Ok(session_tx)
}
