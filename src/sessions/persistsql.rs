use async_trait::async_trait;
use deadpool_sqlite::rusqlite::named_params;
use deadpool_sqlite::rusqlite::OptionalExtension;
use deadpool_sqlite::Pool;

use std::collections::HashSet;
use std::convert::TryInto;

use crate::broker::BrokerId;
use crate::messages::codec::MQTTMessage;
use crate::messages::MQTTMessagePublish;
use crate::messages::MQTTMessagePubrec;
use crate::messages::MQTTMessageType;
use crate::sessions::persist::SessionPersistProvider;
use crate::sessions::SessionError;

pub struct PersistSessionSQL {
    broker_id: BrokerId,
    pool: Pool,
}

impl PersistSessionSQL {
    pub fn new(broker_id: BrokerId, database_pool: Pool) -> PersistSessionSQL {
        PersistSessionSQL {
            broker_id,
            pool: database_pool,
        }
    }
}

#[async_trait]
impl SessionPersistProvider for PersistSessionSQL {
    async fn create_persistent_connection(&mut self) -> Result<u8, SessionError> {
        Ok(0)
    }

    async fn persist_session(&mut self, id: String) -> Result<(i32, i64, bool, u16), SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        let mut generation = 0_i64;
        let mut internal_id = 0_i32;
        let mut clean_session = true;
        let mut next_mid = 1_i32;
        client
            .interact(move |db| {
                let mut stmt = db
                    .prepare("SELECT id, generation, next_mid FROM sessions WHERE name = :name")
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                match stmt.query_row(
                    named_params! {
                    ":name": id,
                    },
                    |row| Ok((
                        row.get::<_, i32>(0),
                        row.get::<_, i64>(1),
                        row.get::<_, i32>(2),
                    )),
                )
                .optional()
                .map_err(|err| {
                    SessionError::Persist(format!("Error checking session: {:?}", err))
                })
                {
                    Ok(result) => {
                        match result {
                            Some(row) => {
                                internal_id = row.0.map_err(|err| {
                                    SessionError::Persist(format!("Error getting value: {:?}", err))
                                })?;
                                generation = row.1.map_err(|err| {
                                    SessionError::Persist(format!("Error getting value: {:?}", err))
                                })?;
                                next_mid = row.2.map_err(|err| {
                                    SessionError::Persist(format!("Error getting value: {:?}", err))
                                })?;
                                clean_session = false;
                            },
                            None => {
                                let mut insert_stmt = db
                                .prepare("INSERT INTO sessions(name, generation) VALUES (:name, :generation) RETURNING id")
                                .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                                internal_id = insert_stmt.query_row(
                                    named_params! {
                                    ":name": id,
                                    ":generation": generation
                                    },
                                    |row|
                                        row.get::<_, i32>(0)
                                )
                                .map_err(|err| {
                                    SessionError::Persist(format!("Error persisting session: {:?}", err))
                                })?;

                            }
                        };

                        Ok((internal_id, generation, clean_session, next_mid as u16))
                    },
                    Err(err) => {
                        Err(err)
                    }
                }
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn load_persistent_subscriptions(
        &mut self,
        session_id: i32,
    ) -> Result<Vec<String>, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        let mut result: Vec<String> = Vec::new();
        client
            .interact(move |db| {
                let mut stmt = db
                    .prepare(
                        "SELECT topics.topic FROM subscriptions
                    INNER JOIN topics ON topics.id = subscriptions.topic_id
                    WHERE subscriptions.session_id = :session_id",
                    )
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                let rows = stmt
                    .query_map(
                        named_params! {
                        ":session_id": session_id,
                        },
                        |row| row.get::<_, std::string::String>(0),
                    )
                    .map_err(|err| {
                        SessionError::Persist(format!(
                            "Loading persistent message failed: {:?}",
                            err
                        ))
                    })?;

                for row in rows {
                    result.push(row.unwrap());
                }

                Ok(result)
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persist_update_session_generation(
        &mut self,
        internal_id: i32,
        generation: i64,
    ) -> Result<i32, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        client
            .interact(move |db| {
                let mut stmt = db
                    .prepare("UPDATE sessions SET generation = :generation, next_mid = 1 WHERE id = :id RETURNING id")
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                stmt.query_row(
                    named_params! {
                    ":id": internal_id,
                    ":generation": generation
                    },
                    |row| row.get::<_, i32>(0),
                )
                .map_err(|err| {
                    SessionError::Persist(format!(
                        "Error persisting session update: {:?}",
                        err
                    ))
                })
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persist_qos_incoming_store(
        &mut self,
        session_id: i32,
        mid: u16,
    ) -> Result<i32, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        client
            .interact(move |db| {
                let mut stmt = db
                    .prepare("INSERT INTO qosin(session_id, mid) VALUES (:session_id, :mid) RETURNING qosin_id")
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                let internal_id = stmt.query_row(
                    named_params! {
                        ":session_id": session_id,
                        ":mid": mid
                        },
                    |row| row.get::<_, i32>(0),
                )
                .map_err(|err| {
                    SessionError::Persist(format!(
                        "Error storing qosin mid: {:?}",
                        err
                    ))
                })?;

                Ok(internal_id)
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persist_qos_incoming_delete(
        &mut self,
        session_id: i32,
        mid: u16,
    ) -> Result<u16, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        client
            .interact(move |db| {
                let mut stmt = db
                    .prepare("DELETE FROM qosin WHERE session_id = :session_id AND mid = :mid")
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                stmt.execute(named_params! {
                ":session_id": session_id,
                ":mid": mid
                })
                .map_err(|err| {
                    SessionError::Persist(format!("Error deleting qosin mid: {:?}", err))
                })?;

                Ok(mid)
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persist_qos_incoming_clear(&mut self, session_id: i32) -> Result<i32, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        client
            .interact(move |db| {
                let mut stmt = db
                    .prepare("DELETE FROM qosin WHERE session_id = :id")
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                stmt.execute(named_params! {
                ":id": session_id,
                })
                .map_err(|err| {
                    SessionError::Persist(format!("Clearing qos messages failed: {:?}", err))
                })?;

                Ok(session_id)
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persist_qos_incoming_load(
        &mut self,
        session_id: i32,
    ) -> Result<HashSet<u16>, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        let mut mids: HashSet<u16> = HashSet::new();
        client
            .interact(move |db| {
                let mut stmt = db
                    .prepare("SELECT mid FROM qosin WHERE session_id = :session_id")
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                let rows = stmt
                    .query_map(
                        named_params! {
                        ":session_id": session_id,
                        },
                        |row| Ok(row.get::<_, i32>(0)),
                    )
                    .map_err(|err| {
                        SessionError::Persist(format!(
                            "Loading persistent qos messages failed: {:?}",
                            err
                        ))
                    })?;

                for row in rows {
                    let r = row.unwrap();
                    let mid: i32 = r.unwrap();

                    mids.insert(mid as u16);
                }

                Ok(mids)
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persist_qos_outgoing_store(
        &mut self,
        session_id: i32,
        mid: u16,
        msg: &MQTTMessage,
    ) -> Result<u16, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        let msg_copy = msg.clone();

        client
            .interact(move |db| {
                let transaction = db.transaction().map_err(|err| {
                    SessionError::Persist(format!("Unable to start transaction: {:}", err))
                })?;

                let imid = mid as i32;
                let mtype: i32;
                let mut message_id: Option<i32> = None;

                match &msg_copy {
                    MQTTMessage::Publish(m) => {
                        mtype = MQTTMessageType::Publish as i32;

                        let mut stmt = transaction
                        .prepare("INSERT INTO messages(qos, topic, payload) VALUES (:qos, :topic, :payload) RETURNING id")
                        .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                        let stored_message_id = stmt.query_row(
                            named_params! {
                            ":qos": m.get_qos() as i16,
                            ":topic": m.get_topic(),
                            ":payload": m.get_message(),
                            },
                            |row| row.get::<_, i32>(0),
                        )
                        .map_err(|err| {
                            SessionError::Persist(format!("QoS outgoing message store failed: {:?}", err))
                        })?;
                        message_id = Some(stored_message_id);
                    }
                    MQTTMessage::PubRec(_) => {
                        mtype = MQTTMessageType::PubRec as i32;
                    }
                    _ => {
                        return Err(SessionError::Persist(
                            "QoS outgoing message unknown message type".to_string(),
                        ));
                    }
                };

                transaction
                .execute(
                    "INSERT INTO qosout(session_id, type, mid, message_id) VALUES (:session_id, :type, :mid, :message_id) RETURNING qosout_id",
                    named_params! {
                        ":session_id": session_id,
                        ":type": mtype,
                        ":mid": imid,
                        ":message_id": message_id,
                        },
                )
                .map_err(|err| {
                    SessionError::Persist(format!("Message retain failed: {:?}", err))
                })?;

                transaction.commit().map_err(|_| {
                    SessionError::Persist("QoS outgoing delete commit failed".to_owned())
                })?;

                Ok(mid)
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persist_qos_outgoing_delete(
        &mut self,
        session_id: i32,
        mid: u16,
    ) -> Result<u16, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        client
            .interact(move |db| {
                let transaction = db.transaction().map_err(|err| {
                    SessionError::Persist(format!("Unable to start transaction: {:}", err))
                })?;

                let mut stmt = transaction
                    .prepare("SELECT message_id FROM qosout WHERE session_id = :session_id AND mid = :mid")
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                let message_id = stmt.query_row(
                    named_params! {
                    ":session_id": session_id,
                    ":mid": mid,
                    },
                    |row| row.get::<_, i32>(0),
                )
                .map_err(|err| {
                    SessionError::Persist(format!("Delete qosout message getting message id failed: {:?}", err))
                })?;
                drop(stmt);

                transaction
                .execute(
                    "DELETE FROM qosout WHERE session_id = :session_id AND mid = :mid",
                    named_params! {
                        ":session_id": session_id,
                        ":mid": mid,
                        },
                )
                .map_err(|err| {
                    SessionError::Persist(format!("Error deleting qosout mid: {:?}", err))
                })?;

                transaction
                .execute(
                    "DELETE FROM messages WHERE id = :id",
                    named_params! {
                        ":id": message_id,
                        },
                )
                .map_err(|err| {
                    SessionError::Persist(format!("Qosout delete message failed: {:?}", err))
                })?;

                transaction.commit().map_err(|_| {
                    SessionError::Persist("QoS outgoing delete commit failed".to_owned())
                })?;

                Ok(mid)
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persist_qos_outgoing_update_dup(
        &mut self,
        session_id: i32,
        mid: u16,
        dup: bool,
    ) -> Result<u16, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        client
            .interact(move |db| {
                let mut stmt = db
                    .prepare("UPDATE qosout SET dup = :dup WHERE session_id = :session_id AND mid = :mid RETURNING qosout_id")
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                stmt.query_row(
                    named_params! {
                    ":session_id": session_id,
                    ":mid": mid,
                    ":dup": dup,
                    },
                    |row| row.get::<_, u16>(0),
                )
                .map_err(|err| {
                    SessionError::Persist(format!(
                        "Loading persistent message failed: {:?}",
                        err
                    ))
                })
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persist_qos_outgoing_clear(&mut self, session_id: i32) -> Result<u16, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        client
            .interact(move |db| {
                let transaction = db.transaction().map_err(|err| {
                    SessionError::Persist(format!("Unable to start transaction: {:}", err))
                })?;

                let mut stmt: deadpool_sqlite::rusqlite::Statement<'_> = transaction
                    .prepare("SELECT message_id FROM qosout WHERE session_id = :session_id")
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                let mid_rows = stmt
                    .query_map(
                        named_params! {
                        ":session_id": session_id,
                        },
                        |row| row.get::<_, i32>(0),
                    )
                    .map_err(|err| {
                        SessionError::Persist(format!(
                            "Loading persistent message failed: {:?}",
                            err
                        ))
                    })?;

                let mut message_ids: Vec<i32> = Vec::new();
                for row in mid_rows {
                    let mid = row.map_err(|err| {
                        SessionError::Persist(format!("Error getting value: {:?}", err))
                    })?;
                    message_ids.push(mid);
                }
                drop(stmt);

                transaction
                    .execute(
                        "DELETE FROM qosout WHERE session_id = :session_id",
                        named_params! {
                        ":session_id": session_id,
                        },
                    )
                    .map_err(|err| {
                        SessionError::Persist(format!("Error clearing qosout: {:?}", err))
                    })?;

                for message_id in message_ids {
                    transaction
                        .execute(
                            "DELETE FROM messages WHERE id = :id",
                            named_params! {
                            ":id": message_id,
                            },
                        )
                        .map_err(|err| {
                            SessionError::Persist(format!(
                                "Qosout clear delete message failed: {:?}",
                                err
                            ))
                        })?;
                }

                transaction.commit().map_err(|_| {
                    SessionError::Persist("Qosout clear delete message failed".to_owned())
                })?;

                Ok(0)
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persist_update_session_mid(
        &mut self,
        internal_id: i32,
        mid: u16,
    ) -> Result<u16, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        client
            .interact(move |db| {
                let mut stmt = db
                    .prepare("UPDATE sessions SET next_mid = :next_mid WHERE id = :id RETURNING next_mid")
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                stmt.query_row(
                    named_params! {
                    ":next_mid": mid,
                    ":id": internal_id,
                    },
                    |row| row.get::<_, u16>(0),
                )
                .map_err(|err| {
                    SessionError::Persist(format!(
                        "Updating session mid failed: {:?}",
                        err
                    ))
                })
            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }

    async fn persistent_qosout_load(
        &mut self,
        session_id: i32,
    ) -> Result<Vec<MQTTMessage>, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        client
            .interact(move |db| {
                let mut stmt = db
                    .prepare("SELECT qosout_id,message_id,type,mid,dup FROM qosout WHERE session_id = :session_id ORDER BY received ASC",)
                    .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

                let rows = stmt.query_map(
                    named_params! {
                    ":session_id": session_id,
                    },
                    |row|                     Ok((
                        row.get::<_, i32>(0),
                        row.get::<_, Option<i32>>(1),
                        row.get::<_, i32>(2),
                        row.get::<_, i32>(3),row.get::<_, bool>(4),
                    )),
                )
                .map_err(|err| {
                    SessionError::Persist(format!(
                        "Loading session QoS queue failed: {:?}",
                        err
                    ))
                })?;

                struct MessageMeta {
                    message_id: Option<i32>,
                    message_type: i32,
                    message_mid: i32,
                    message_dup: bool
                }
                let mut messages_meta: Vec<MessageMeta> = Vec::new();

                for row in rows {
                    let r = row.map_err(|err| {
                        SessionError::Persist(format!("Error getting value: {:?}", err))
                    })?;
                    let message_id: Option<i32> = r.1.map_err(|err| {
                        SessionError::Persist(format!("Error getting value: {:?}", err))
                    })?;
                    let message_type: i32 = r.2.map_err(|err| {
                        SessionError::Persist(format!("Error getting value: {:?}", err))
                    })?;
                    let message_mid: i32 = r.3.map_err(|err| {
                        SessionError::Persist(format!("Error getting value: {:?}", err))
                    })?;
                    let message_dup: bool = r.4.map_err(|err| {
                        SessionError::Persist(format!("Error getting value: {:?}", err))
                    })?;

                    messages_meta.push( MessageMeta {
                        message_id,
                        message_type,
                        message_mid,
                        message_dup,
                    })
                }
                drop(stmt);

                let mut messages: Vec<MQTTMessage> = Vec::new();
                for message in messages_meta {
                    match message.message_type.try_into() {
                        Ok(MQTTMessageType::Publish) => {
                            if let Some(id) = message.message_id {
                                let mut pubmsg = client_load_persistent_message(db, id)?;
                                pubmsg.set_identifier(message.message_mid as u16);
                                pubmsg.set_dup(message.message_dup);
                                messages.push(MQTTMessage::Publish(pubmsg));
                            } else {
                                return Err(SessionError::Persist(
                                    "Error loading qosout messages, no id for publish message"
                                        .to_string(),
                                ));
                            }
                        }
                        Ok(MQTTMessageType::PubRec) => {
                            let mut pubrec = MQTTMessagePubrec::new();
                            pubrec.set_identifier(message.message_mid as u16);
                            messages.push(MQTTMessage::PubRec(pubrec));
                        }
                        _ => {
                            return Err(SessionError::Persist(
                                "Error loading qosout messages, unkown message type".to_string(),
                            ));
                        }
                    };
                }

                Ok(messages)

            })
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }
}

impl PersistSessionSQL {
    async fn load_persistent_message(
        &mut self,
        message_id: i32,
    ) -> Result<MQTTMessagePublish, SessionError> {
        let client =
            &self.pool.get().await.map_err(|err| {
                SessionError::Persist(format!("Failed to get db client: {:?}", err))
            })?;

        client
            .interact(move |db| client_load_persistent_message(db, message_id))
            .await
            .map_err(|err| SessionError::Persist(format!("Failed to run query: {:?}", err)))?
    }
}

fn client_load_persistent_message(
    db: &mut deadpool_sqlite::rusqlite::Connection,
    message_id: i32,
) -> Result<MQTTMessagePublish, SessionError> {
    let mut stmt = db
        .prepare("SELECT id,qos,topic,payload FROM messages WHERE id = :id")
        .map_err(|err| SessionError::Persist(format!("{:}", err)))?;

    stmt.query_row(
        named_params! {
        ":id": message_id,
        },
        |row| {
            let qos = row.get::<_, i16>(1)?;
            let topic = row.get::<_, std::string::String>(2)?;
            let data = row.get::<_, Vec<u8>>(3)?;

            let mut message = MQTTMessagePublish::new();
            message.set_topic(topic);
            message.set_qos(qos as u8);
            message.set_message(data);

            Ok(message)
        },
    )
    .map_err(|err| SessionError::Persist(format!("Loading persistent message failed: {:?}", err)))
}
