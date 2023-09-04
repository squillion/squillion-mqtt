#[cfg(test)]
mod testpersist {
    use std::fs;
    use std::path::Path;

    use crate::broker::persist;
    use crate::config;
    use crate::messages::MQTTMessagePublish;
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

    async fn create_session(
        sessionprovider: &mut Box<dyn sessions::persist::SessionPersistProvider>,
    ) -> i32 {
        let session1: String = "session1".to_string();

        let first_session = sessionprovider.persist_session(session1.clone()).await;
        assert!(first_session.is_ok());
        let session_id = if let Ok(session) = first_session {
            let (id, _, _, _) = session;
            id
        } else {
            -1
        };

        assert!(session_id > 0);

        session_id
    }

    #[tokio::test]
    async fn test_subscribe() {
        let (mut provider, mut sessionprovider) = createdb().await;

        let session_id = create_session(&mut sessionprovider).await;

        let testtopic1: String = "testtopic1".to_string();
        let topic = provider.persist_topic(testtopic1.clone()).await;
        assert!(topic.is_ok());
        let topic_id = topic.unwrap();

        let subscribe = provider.persist_subscribe(session_id, topic_id, 0).await;
        assert!(subscribe.is_ok());

        let subscriptions = provider.load_persistent_subscriptions(topic_id).await;
        assert!(subscriptions.is_ok());

        let sub_list = subscriptions.unwrap();
        assert_eq!(sub_list.len(), 1);

        assert_eq!(sub_list.get(0).unwrap().0.session, "session1");
        assert_eq!(sub_list.get(0).unwrap().0.internal_id, session_id);
        assert_eq!(sub_list.get(0).unwrap().1, 0);
    }

    #[tokio::test]
    async fn test_subscribe_update() {
        let (mut provider, mut sessionprovider) = createdb().await;

        let session_id = create_session(&mut sessionprovider).await;

        let testtopic1: String = "testtopic1".to_string();
        let topic = provider.persist_topic(testtopic1.clone()).await;
        assert!(topic.is_ok());
        let topic_id = topic.unwrap();

        let subscribe = provider.persist_subscribe(session_id, topic_id, 0).await;
        assert!(subscribe.is_ok());

        let update = provider
            .persist_update_subscribe(session_id, topic_id, 2)
            .await;
        assert!(update.is_ok());

        let subscriptions = provider.load_persistent_subscriptions(topic_id).await;
        assert!(subscriptions.is_ok());

        let sub_list = subscriptions.unwrap();
        assert_eq!(sub_list.len(), 1);

        assert_eq!(sub_list.get(0).unwrap().0.session, "session1");
        assert_eq!(sub_list.get(0).unwrap().0.internal_id, session_id);
        assert_eq!(sub_list.get(0).unwrap().1, 2);
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let (mut provider, mut sessionprovider) = createdb().await;

        let session_id = create_session(&mut sessionprovider).await;

        let testtopic1: String = "testtopic1".to_string();
        let topic = provider.persist_topic(testtopic1.clone()).await;
        assert!(topic.is_ok());
        let topic_id = topic.unwrap();

        let subscribe = provider.persist_subscribe(session_id, topic_id, 0).await;
        assert!(subscribe.is_ok());

        let subscriptions = provider.load_persistent_subscriptions(topic_id).await;
        assert!(subscriptions.is_ok());

        let sub_list = subscriptions.unwrap();
        assert_eq!(sub_list.len(), 1);

        // Unsubscribe
        let unsubscribe = provider.persist_unsubscribe(session_id, topic_id).await;
        assert!(unsubscribe.is_ok());

        let subscriptions_after = provider.load_persistent_subscriptions(topic_id).await;
        assert!(subscriptions_after.is_ok());

        let sub_list_after = subscriptions_after.unwrap();
        assert_eq!(sub_list_after.len(), 0);
    }

    #[tokio::test]
    async fn test_topic_persist_retain_message() {
        let (mut provider, _) = createdb().await;

        let testtopic1: String = "testtopic1".to_string();
        let topic = provider.persist_topic(testtopic1.clone()).await;
        assert!(topic.is_ok());
        let topic_id = topic.unwrap();

        let msg_bytes = "Test message".as_bytes().to_vec();
        let mut msg = MQTTMessagePublish::new();
        msg.set_qos(1);
        msg.set_topic(testtopic1.clone());
        msg.set_message(msg_bytes.clone());

        let message = provider.persist_retain(topic_id, &msg).await;
        assert!(message.is_ok());
        let message_id = message.unwrap();

        let loadedtopics = provider.load_persistent_topics().await;
        assert!(loadedtopics.is_ok());

        if let Ok(topics) = loadedtopics {
            assert_eq!(topics.len(), 1);
            assert_eq!(topics.get(0).unwrap().topic_name, testtopic1);
            assert_eq!(topics.get(0).unwrap().retained_message_id, Some(message_id));
        }

        let retained_message = provider.load_persistent_message(message_id).await;
        assert!(retained_message.is_ok());

        if let Ok(loaded_message) = retained_message {
            assert_eq!(*loaded_message.get_topic(), testtopic1);
            assert_eq!(loaded_message.get_qos(), 1);
            assert_eq!(*loaded_message.get_message(), msg_bytes);
        }
    }

    #[tokio::test]
    async fn test_topic_persist_retain_message_delete() {
        let (mut provider, _) = createdb().await;

        let testtopic1: String = "testtopic1".to_string();
        let topic = provider.persist_topic(testtopic1.clone()).await;
        assert!(topic.is_ok());
        let topic_id = topic.unwrap();

        let msg_bytes = "Test message".as_bytes().to_vec();
        let mut msg = MQTTMessagePublish::new();
        msg.set_qos(0);
        msg.set_topic(testtopic1.clone());
        msg.set_message(msg_bytes.clone());

        let message = provider.persist_retain(topic_id, &msg).await;
        assert!(message.is_ok());
        let message_id = message.unwrap();

        let retained_message = provider.load_persistent_message(message_id).await;
        assert!(retained_message.is_ok());

        if let Ok(loaded_message) = retained_message {
            assert_eq!(*loaded_message.get_topic(), testtopic1.clone());
            assert_eq!(loaded_message.get_qos(), 0);
            assert_eq!(*loaded_message.get_message(), msg_bytes);
        }

        let delete_message = provider.persist_delete_retain(topic_id).await;
        assert!(delete_message.is_ok());

        let loadedtopics = provider.load_persistent_topics().await;
        assert!(loadedtopics.is_ok());

        if let Ok(topics) = loadedtopics {
            assert_eq!(topics.len(), 1);
            assert_eq!(topics.get(0).unwrap().topic_name, testtopic1);
            assert_eq!(topics.get(0).unwrap().retained_message_id, None);
        }

        let retained_message = provider.load_persistent_message(message_id).await;
        assert!(retained_message.is_err());
    }

    #[tokio::test]
    async fn test_persist_qos_incoming_store() {
        let (_, mut sessionprovider) = createdb().await;

        let session_id = create_session(&mut sessionprovider).await;

        assert!(sessionprovider
            .persist_qos_incoming_store(session_id, 2)
            .await
            .is_ok());
        assert!(sessionprovider
            .persist_qos_incoming_store(session_id, 5)
            .await
            .is_ok());

        let mut qos_incoming = sessionprovider.persist_qos_incoming_load(session_id).await;
        assert!(qos_incoming.is_ok());
        if let Ok(qos_ids) = qos_incoming {
            assert_eq!(qos_ids.len(), 2);
            assert!(!qos_ids.contains(&1));
            assert!(qos_ids.contains(&2));
            assert!(qos_ids.contains(&5));
        }

        let clear = sessionprovider.persist_qos_incoming_clear(session_id).await;
        assert!(clear.is_ok());

        qos_incoming = sessionprovider.persist_qos_incoming_load(session_id).await;
        assert!(qos_incoming.is_ok());
        if let Ok(qos_ids) = qos_incoming {
            assert_eq!(qos_ids.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_persist_qos_incoming_delete() {
        let (_, mut sessionprovider) = createdb().await;

        let session_id = create_session(&mut sessionprovider).await;

        assert!(sessionprovider
            .persist_qos_incoming_store(session_id, 10)
            .await
            .is_ok());
        assert!(sessionprovider
            .persist_qos_incoming_store(session_id, 11)
            .await
            .is_ok());
        assert!(sessionprovider
            .persist_qos_incoming_store(session_id, 12)
            .await
            .is_ok());

        let mut qos_incoming = sessionprovider.persist_qos_incoming_load(session_id).await;
        assert!(qos_incoming.is_ok());
        if let Ok(qos_ids) = qos_incoming {
            assert_eq!(qos_ids.len(), 3);
            assert!(qos_ids.contains(&10));
            assert!(qos_ids.contains(&11));
            assert!(qos_ids.contains(&12));
        }

        let delete = sessionprovider
            .persist_qos_incoming_delete(session_id, 11)
            .await;
        assert!(delete.is_ok());
        qos_incoming = sessionprovider.persist_qos_incoming_load(session_id).await;
        assert!(qos_incoming.is_ok());
        if let Ok(qos_ids) = qos_incoming {
            assert_eq!(qos_ids.len(), 2);
            assert!(qos_ids.contains(&10));
            assert!(!qos_ids.contains(&11));
            assert!(qos_ids.contains(&12));
        }

        assert!(sessionprovider
            .persist_qos_incoming_delete(session_id, 10)
            .await
            .is_ok());
        assert!(sessionprovider
            .persist_qos_incoming_delete(session_id, 12)
            .await
            .is_ok());
        qos_incoming = sessionprovider.persist_qos_incoming_load(session_id).await;
        assert!(qos_incoming.is_ok());
        if let Ok(qos_ids) = qos_incoming {
            assert_eq!(qos_ids.len(), 0);
            assert!(!qos_ids.contains(&10));
            assert!(!qos_ids.contains(&11));
            assert!(!qos_ids.contains(&12));
        }

        let clear = sessionprovider.persist_qos_incoming_clear(session_id).await;
        assert!(clear.is_ok());

        qos_incoming = sessionprovider.persist_qos_incoming_load(session_id).await;
        assert!(qos_incoming.is_ok());
        if let Ok(qos_ids) = qos_incoming {
            assert_eq!(qos_ids.len(), 0);
        }
    }
}
