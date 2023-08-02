use async_trait::async_trait;

use crate::auth::AuthProvider;
use crate::broker::BrokerId;
use crate::messages::ReturnCode;

use super::AuthResponse;

pub struct TestAuth {}

impl TestAuth {
    pub fn new() -> Self {
        TestAuth {}
    }
}

#[async_trait]
impl AuthProvider for TestAuth {
    fn initialize(&mut self) -> Result<(), ()> {
        Ok(())
    }

    async fn check_password(&self, user: &str, _password: &str) -> AuthResponse {
        if !user.is_empty() {
            AuthResponse {
                return_code: ReturnCode::Accepted,
                tenant: Some(user.to_string()),
            }
        } else {
            let broker = BrokerId::test_broker();
            AuthResponse {
                return_code: ReturnCode::Accepted,
                tenant: broker.tenant_id,
            }
        }
    }
}
