use async_trait::async_trait;
use std::collections::HashMap;
use std::fs;

use base64::{engine::general_purpose, Engine as _};
use crypto::digest::Digest;
use crypto::sha1::Sha1;

use crate::auth::AuthProvider;
use crate::config;

fn check_sha_password(password: &str, hash: &str) -> bool {
    let mut m = Sha1::new();
    let mut out: [u8; 20] = [0; 20];

    m.input_str(password);
    m.result(&mut out);

    general_purpose::STANDARD.encode(out).eq(hash)
}

pub struct PasswordAuth {
    users: HashMap<String, String>,
}

impl PasswordAuth {
    pub fn new() -> Self {
        PasswordAuth {
            users: HashMap::new(),
        }
    }

    pub fn load_password_file(&mut self, filename: &str) {
        let contents = fs::read_to_string(filename);
        match contents {
            Ok(content) => {
                let users = content.lines();
                for user in users {
                    let t: Vec<_> = user.split(':').collect();
                    if t.len() != 2 {
                        println!("Error parsing: {}", user);
                    } else {
                        self.users.insert(t[0].to_string(), t[1].to_string());
                    }
                }
            }
            Err(e) => {
                println!("Unable to open file: {}", e);
            }
        }
    }

    pub fn check_password_internal(&self, user: &str, password: &str) -> Option<String> {
        match self.users.get(user) {
            Some(hash) => {
                if let Some(h) = hash.strip_prefix("{SHA}") {
                    if check_sha_password(password, h) {
                        Some("default".to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

#[async_trait]
impl AuthProvider for PasswordAuth {
    fn initialize(&mut self) -> Result<(), ()> {
        let filename = config::get_string("password_file").unwrap();
        self.load_password_file(&filename);
        Ok(())
    }

    async fn check_password(&self, user: &str, password: &str) -> Option<String> {
        self.check_password_internal(user, password)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha_password() {
        assert!(check_sha_password(
            "password",
            "W6ph5Mm5Pz8GgiULbPgzG37mj9g="
        ));
        assert!(!check_sha_password(
            "notpassword",
            "W6ph5Mm5Pz8GgiULbPgzG37mj9g="
        ));
    }

    #[test]
    fn test_password_file() {
        let mut pa = PasswordAuth::new();
        pa.load_password_file("tests/users.txt");

        assert_eq!(
            pa.check_password_internal("user1", "password1"),
            Some("default".to_string())
        );
        assert_eq!(pa.check_password_internal("user1", "password2"), None);
        assert_eq!(
            pa.check_password_internal("user5", "password5"),
            Some("default".to_string())
        );
    }
}
