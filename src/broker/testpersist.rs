#[cfg(test)]
mod testpersist {
    use std::fs;
    use std::path::Path;

    use super::*;
    use crate::broker::persist;
    use crate::config;
    use crate::{broker, sessions};

    pub fn initialize() {
        delete_databases();
        config::load_sqlite_test_config().unwrap();
    }

    async fn createdb() -> (
        Box<dyn persist::PersistProvider>,
        Box<dyn sessions::persist::SessionPersistProvider>,
    ) {
        initialize();

        let brokerid = broker::BrokerId::test_broker();

        let pool = persist::get_persist_pool(&brokerid);
        let mut provider = persist::get_persist_provider(brokerid.clone(), Some(pool.clone()));

        assert_eq!(provider.create_persistent_connection().await, Ok(0));
        assert_eq!(provider.init_persistent_store().await, Ok(0));

        let sessionprovider = sessions::persist::get_session_persist_provider(brokerid, Some(pool));

        (provider, sessionprovider)
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
        let (mut provider, _) = createdb().await;

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

    #[tokio::test]
    async fn test_session() {
        let (_, mut sessionprovider) = createdb().await;

        let session1: String = "session1".to_string();

        let first_session = sessionprovider.persist_session(session1.clone()).await;
        assert!(first_session.is_ok());
        let first_id = if let Ok(session) = first_session {
            let (id, generation, clean_session, next_mid) = session;
            assert_eq!(id, 1);
            assert_eq!(generation, 0);
            assert_eq!(clean_session, true);
            assert_eq!(next_mid, 1);

            id
        } else {
            -1
        };

        let second_session = sessionprovider.persist_session(session1.clone()).await;
        assert!(second_session.is_ok());
        if let Ok(session) = second_session {
            let (id, generation, clean_session, next_mid) = session;
            assert_eq!(id, first_id);
            assert_eq!(generation, 0);
            assert_eq!(clean_session, false);
            assert_eq!(next_mid, 1);
        }
    }

    #[tokio::test]
    async fn test_session_update_generation() {
        let (_, mut sessionprovider) = createdb().await;

        let session1: String = "session1".to_string();

        let first_session = sessionprovider.persist_session(session1.clone()).await;
        assert!(first_session.is_ok());
        let first_id = if let Ok(session) = first_session {
            let (id, generation, clean_session, next_mid) = session;
            assert_eq!(id, 1);
            assert_eq!(generation, 0);
            assert_eq!(clean_session, true);
            assert_eq!(next_mid, 1);

            id
        } else {
            -1
        };

        assert!(sessionprovider
            .persist_update_session_generation(first_id, 5)
            .await
            .is_ok());

        let second_session = sessionprovider.persist_session(session1.clone()).await;
        assert!(second_session.is_ok());
        if let Ok(session) = second_session {
            let (id, generation, clean_session, next_mid) = session;
            assert_eq!(id, first_id);
            assert_eq!(generation, 5);
            assert_eq!(clean_session, false);
            assert_eq!(next_mid, 1);
        }
    }

    #[tokio::test]
    async fn test_session_update_mid() {
        let (_, mut sessionprovider) = createdb().await;

        let session1: String = "session1".to_string();

        let first_session = sessionprovider.persist_session(session1.clone()).await;
        assert!(first_session.is_ok());
        let first_id = if let Ok(session) = first_session {
            let (id, generation, clean_session, next_mid) = session;
            assert_eq!(id, 1);
            assert_eq!(generation, 0);
            assert_eq!(clean_session, true);
            assert_eq!(next_mid, 1);

            id
        } else {
            -1
        };

        assert!(sessionprovider
            .persist_update_session_mid(first_id, 12)
            .await
            .is_ok());

        let second_session = sessionprovider.persist_session(session1.clone()).await;
        assert!(second_session.is_ok());
        if let Ok(session) = second_session {
            let (id, generation, clean_session, next_mid) = session;
            assert_eq!(id, first_id);
            assert_eq!(generation, 0);
            assert_eq!(clean_session, false);
            assert_eq!(next_mid, 12);
        }
    }
}
