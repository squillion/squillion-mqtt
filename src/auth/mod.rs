pub mod pwd_file;
pub mod pwd_list;
pub mod testauth;

use async_trait::async_trait;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures_util::stream::StreamExt;

use crate::config;
use crate::messages;
use crate::messages::ReturnCode;

use self::pwd_file::PasswordAuth;
use self::pwd_list::PasswordListAuth;
use self::testauth::TestAuth;

pub struct AuthResponse {
    pub return_code: ReturnCode,
    pub tenant: Option<String>,
}

pub type AuthTx = mpsc::UnboundedSender<AuthRequest>;

// Shorthand for the transmit half of the message channel.
type Tx = oneshot::Sender<AuthResponse>;

pub struct AuthRequest {
    pub response_channel: Tx,
    pub username: String,
    pub password: String,
}

impl AuthRequest {
    pub fn new(username: String, password: String, response_channel: Tx) -> AuthRequest {
        AuthRequest {
            response_channel,
            username,
            password,
        }
    }
}

#[async_trait]
trait AuthProvider: Sync + Send {
    fn initialize(&mut self) -> Result<(), ()>;
    async fn check_password(&self, user: &str, password: &str) -> AuthResponse;
}

pub struct AuthService {
    logger: slog::Logger,
    auth_tx: Option<mpsc::UnboundedSender<AuthRequest>>,
}

impl AuthService {
    pub fn new(logger: slog::Logger) -> AuthService {
        AuthService {
            logger,
            auth_tx: None,
        }
    }

    pub fn get_tx(&mut self) -> Option<&mpsc::UnboundedSender<AuthRequest>> {
        self.auth_tx.as_ref()
    }

    pub fn process(&mut self) -> Result<(), &str> {
        let (auth_tx, mut auth_rx): (
            mpsc::UnboundedSender<AuthRequest>,
            mpsc::UnboundedReceiver<AuthRequest>,
        ) = mpsc::unbounded();

        self.auth_tx = Some(auth_tx);

        let auth_method: String;
        if let Some(am) = config::get_string("auth_method") {
            auth_method = am;
        } else {
            return Err("Auth method not configured");
        };

        let mut auth_provider: Box<dyn AuthProvider> = match auth_method.as_str() {
            "pwdfile" => Box::new(PasswordAuth::new()),
            "pwdlist" => Box::new(PasswordListAuth::new()),
            "test" => Box::new(TestAuth::new()),
            _ => Box::new(PasswordListAuth::new()),
        };
        if let Err(_err) = auth_provider.initialize() {
            slog::error!(self.logger, "Error initializing auth provider");
            return Err("Error initializing auth provider");
        }

        let logger = self.logger.clone();

        let _auth_spawn = tokio::spawn(async move {
            loop {
                match auth_rx.next().await {
                    Some(msg) => {
                        let auth_response = auth_provider
                            .check_password(&msg.username, &msg.password)
                            .await;

                        if auth_response.return_code != messages::ReturnCode::Accepted {
                            slog::debug!(logger, "Client auth failed. Username: {}", msg.username);
                        }

                        if msg.response_channel.send(auth_response).is_err() {
                            slog::warn!(logger, "Unable to send auth response");
                        }
                    }
                    _ => panic!("Error receiving auth request"),
                }
            }
        });

        Ok(())
    }
}
