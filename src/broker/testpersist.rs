#[cfg(test)]
mod testpersist {
    use std::fs;
    use std::path::Path;

    use super::*;
    use crate::broker;
    use crate::broker::persist;
    use crate::config;

    pub fn initialize() {
        delete_databases();
        config::load_sqlite_test_config().unwrap();
    }

    async fn createdb() -> Box<dyn persist::PersistProvider> {
        initialize();

        let brokerid = broker::BrokerId::test_broker();

        let pool = persist::get_persist_pool(&brokerid);
        let mut provider = persist::get_persist_provider(brokerid, Some(pool));

        assert_eq!(provider.create_persistent_connection().await, Ok(0));
        assert_eq!(provider.init_persistent_store().await, Ok(0));

        provider
    }

    fn delete_databases() {
        let persist_path = config::get_string("persist_data_store").unwrap();
        let path = Path::new(&persist_path).join("tenants");
        let _ = fs::remove_dir_all(&path);
        let none_path = Path::new(&persist_path).join("none");
        let _ = fs::remove_dir_all(&none_path);
    }

    #[tokio::test]
    async fn test_create() {
        initialize();

        let brokerid = broker::BrokerId::test_broker();

        let pool = persist::get_persist_pool(&brokerid);
        let mut provider = persist::get_persist_provider(brokerid, Some(pool));

        assert_eq!(provider.create_persistent_connection().await, Ok(0));
        assert_eq!(provider.init_persistent_store().await, Ok(0));
    }

    #[tokio::test]
    async fn test_topic() {
        let mut provider = createdb().await;

        let testtopic1: String = "testtopic1".to_string();
        assert!(provider.persist_topic(testtopic1.clone()).await.is_ok());

        let loadedtopics = provider.load_persistent_topics().await;
        assert!(loadedtopics.is_ok());

        if let Ok(topics) = loadedtopics {
            assert_eq!(topics.len(), 1);
            assert_eq!(topics.get(0).unwrap().topic_name, testtopic1);
            assert_eq!(topics.get(0).unwrap().retained_message_id, None);
        }
    }
}
