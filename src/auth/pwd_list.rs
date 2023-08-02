use async_trait::async_trait;
use std::collections::HashMap;

use crate::auth::AuthProvider;
use crate::config;
use crate::messages::ReturnCode;

use super::AuthResponse;

pub struct PasswordListAuth {
    users: HashMap<String, String>,
}

impl PasswordListAuth {
    pub fn new() -> Self {
        PasswordListAuth {
            users: HashMap::new(),
        }
    }

    pub fn load_passwords(&mut self) {
        let passwords = config::get_users();
        for user in passwords {
            self.users.insert(user.username, user.password);
        }
    }

    pub fn check_password_internal(&self, user: &str, password: &str) -> ReturnCode {
        match self.users.get(user) {
            Some(pwd) => {
                if password == pwd {
                    ReturnCode::Accepted
                } else {
                    ReturnCode::BadUsernameOrPassword
                }
            }
            None => ReturnCode::BadUsernameOrPassword,
        }
    }
}

#[async_trait]
impl AuthProvider for PasswordListAuth {
    fn initialize(&mut self) -> Result<(), ()> {
        self.load_passwords();
        Ok(())
    }

    async fn check_password(&self, user: &str, password: &str) -> AuthResponse {
        let return_code = self.check_password_internal(user, password);
        AuthResponse {
            return_code,
            tenant: None,
        }
    }
}
