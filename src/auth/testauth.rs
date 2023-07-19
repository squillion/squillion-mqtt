use async_trait::async_trait;

use crate::auth::AuthProvider;
use crate::broker::BrokerId;

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

    async fn check_password(&self, user: &str, _password: &str) -> Option<String> {
        if !user.is_empty() {
            Some(user.to_string())
        } else {
            let broker = BrokerId::test_broker();
            Some(broker.tenant_id)
        }
    }
}
