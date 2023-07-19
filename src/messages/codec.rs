use bytes::Bytes;
use bytes::{BufMut, BytesMut};

use std::io::ErrorKind;
use tokio_util::codec::{Decoder, Encoder};

use super::length;
use super::length::read_size_check;
use crate::messages::MQTTMessageConnack;
use crate::messages::MQTTMessageConnect;
use crate::messages::MQTTMessageDisconnect;
use crate::messages::MQTTMessagePingReq;
use crate::messages::MQTTMessagePingResp;
use crate::messages::MQTTMessagePuback;
use crate::messages::MQTTMessagePubcomp;
use crate::messages::MQTTMessagePublish;
use crate::messages::MQTTMessagePubrec;
use crate::messages::MQTTMessagePubrel;
use crate::messages::MQTTMessageSuback;
use crate::messages::MQTTMessageSubscribe;
use crate::messages::MQTTMessageType;
use crate::messages::MQTTMessageUnsuback;
use crate::messages::MQTTMessageUnsubscribe;

#[derive(Clone)]
pub enum MQTTMessage {
    Connect(MQTTMessageConnect),
    ConnAck(MQTTMessageConnack),
    Publish(MQTTMessagePublish),
    PubAck(MQTTMessagePuback),
    PubRec(MQTTMessagePubrec),
    PubRel(MQTTMessagePubrel),
    PubComp(MQTTMessagePubcomp),
    Subscribe(MQTTMessageSubscribe),
    SubAck(MQTTMessageSuback),
    Unsubscribe(MQTTMessageUnsubscribe),
    UnsubAck(MQTTMessageUnsuback),
    PingReq(MQTTMessagePingReq),
    PingResp(MQTTMessagePingResp),
    Disconnect(MQTTMessageDisconnect),
}

pub struct MqttCodec {
    logger: slog::Logger,
}

impl MqttCodec {
    pub fn new(logger: slog::Logger) -> MqttCodec {
        MqttCodec { logger }
    }
}

impl Encoder<MQTTMessage> for MqttCodec {
    //type Item = MQTTMessage;
    type Error = std::io::Error;

    fn encode(&mut self, message: MQTTMessage, buf: &mut BytesMut) -> Result<(), Self::Error> {
        match message {
            MQTTMessage::Publish(m) => {
                let bytes: Vec<u8> = m.to_bytes();
                buf.reserve(bytes.len());
                buf.put(Bytes::from(bytes));
            }
            MQTTMessage::ConnAck(m) => {
                let bytes: Vec<u8> = m.to_bytes();
                buf.reserve(bytes.len());
                buf.put(Bytes::from(bytes));
            }
            MQTTMessage::SubAck(m) => {
                let bytes: Vec<u8> = m.to_bytes();
                buf.reserve(bytes.len());
                buf.put(Bytes::from(bytes));
            }
            MQTTMessage::UnsubAck(m) => {
                let bytes: Vec<u8> = m.to_bytes();
                buf.reserve(bytes.len());
                buf.put(Bytes::from(bytes));
            }
            MQTTMessage::PingResp(m) => {
                let bytes: Vec<u8> = m.to_bytes();
                buf.reserve(bytes.len());
                buf.put(Bytes::from(bytes));
            }
            MQTTMessage::PubAck(m) => {
                let bytes: Vec<u8> = m.to_bytes();
                buf.reserve(bytes.len());
                buf.put(Bytes::from(bytes));
            }
            MQTTMessage::PubRec(m) => {
                let bytes: Vec<u8> = m.to_bytes();
                buf.reserve(bytes.len());
                buf.put(Bytes::from(bytes));
            }
            MQTTMessage::PubRel(m) => {
                let bytes: Vec<u8> = m.to_bytes();
                buf.reserve(bytes.len());
                buf.put(Bytes::from(bytes));
            }
            MQTTMessage::PubComp(m) => {
                let bytes: Vec<u8> = m.to_bytes();
                buf.reserve(bytes.len());
                buf.put(Bytes::from(bytes));
            }
            _ => slog::error!(self.logger, "Unknown msg to send"),
        }

        Ok(())
    }
}

impl Decoder for MqttCodec {
    type Item = MQTTMessage;
    type Error = std::io::Error;

    // Find the next line in buf!
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            return Ok(None);
        }
        let msgtype = buf[0] >> 4;

        let mut len;
        let lensize;
        if let Ok(length_read) = read_size_check(&buf[1..]) {
            if let Some((l, ls)) = length_read {
                len = l;
                lensize = ls;
            } else {
                // More data needed
                return Ok(None);
            }
        } else {
            slog::warn!(self.logger, "Decoder: Invalid message length");
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Invalid message length",
            ));
        }
        if len > length::MAX_LENGTH {
            slog::warn!(self.logger, "Message exceeds max length");
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Decoder: Message exceeds max length",
            ));
        }
        len = len + lensize + 1;

        if buf.capacity() < len {
            buf.reserve(len - buf.capacity());
        }

        if len <= buf.len() {
            let msg = buf.split_to(len);

            if msgtype == MQTTMessageType::Connect as u8 {
                let mut cm = MQTTMessageConnect::new();

                match cm.from_bytes(&msg) {
                    Ok(_) => Ok(Some(MQTTMessage::Connect(cm))),
                    Err(err) => {
                        slog::warn!(self.logger, "CONNECT decode: {}", err);
                        Err(err)
                    }
                }
            } else if msgtype == MQTTMessageType::Subscribe as u8 {
                let mut submsg = MQTTMessageSubscribe::new();
                match submsg.from_bytes(&msg) {
                    Ok(_) => Ok(Some(MQTTMessage::Subscribe(submsg))),
                    Err(err) => {
                        slog::warn!(self.logger, "SUBSCRIBE decode: {}", err);
                        Err(err)
                    }
                }
            } else if msgtype == MQTTMessageType::Unsubscribe as u8 {
                let mut unsubmsg = MQTTMessageUnsubscribe::new();
                match unsubmsg.from_bytes(&msg) {
                    Ok(_) => Ok(Some(MQTTMessage::Unsubscribe(unsubmsg))),
                    Err(err) => {
                        slog::warn!(self.logger, "UNSUBSCRIBE decode: {}", err);
                        Err(err)
                    }
                }
            } else if msgtype == MQTTMessageType::Publish as u8 {
                let mut publishmsg = MQTTMessagePublish::new();
                match publishmsg.from_bytes(&msg) {
                    Ok(_) => Ok(Some(MQTTMessage::Publish(publishmsg))),
                    Err(err) => {
                        slog::warn!(self.logger, "PUBLISH decode: {}", err);
                        Err(err)
                    }
                }
            } else if msgtype == MQTTMessageType::PingReq as u8 {
                let mut pingreqmsg = MQTTMessagePingReq::new();
                match pingreqmsg.from_bytes(&msg) {
                    Ok(_) => Ok(Some(MQTTMessage::PingReq(pingreqmsg))),
                    Err(err) => {
                        slog::warn!(self.logger, "PINGREQ decode: {}", err);
                        Err(err)
                    }
                }
            } else if msgtype == MQTTMessageType::PubAck as u8 {
                let mut pubackmsg = MQTTMessagePuback::new();
                match pubackmsg.from_bytes(&msg) {
                    Ok(_) => Ok(Some(MQTTMessage::PubAck(pubackmsg))),
                    Err(err) => {
                        slog::warn!(self.logger, "PUBACK decode: {}", err);
                        Err(err)
                    }
                }
            } else if msgtype == MQTTMessageType::PubRec as u8 {
                let mut pubrecmsg = MQTTMessagePubrec::new();
                match pubrecmsg.from_bytes(&msg) {
                    Ok(_) => Ok(Some(MQTTMessage::PubRec(pubrecmsg))),
                    Err(err) => {
                        slog::warn!(self.logger, "PUBREC decode: {}", err);
                        Err(err)
                    }
                }
            } else if msgtype == MQTTMessageType::PubRel as u8 {
                let mut pubrelmsg = MQTTMessagePubrel::new();
                match pubrelmsg.from_bytes(&msg) {
                    Ok(_) => Ok(Some(MQTTMessage::PubRel(pubrelmsg))),
                    Err(err) => {
                        slog::warn!(self.logger, "PUBREL decode: {}", err);
                        Err(err)
                    }
                }
            } else if msgtype == MQTTMessageType::PubComp as u8 {
                let mut pubcompmsg = MQTTMessagePubcomp::new();
                match pubcompmsg.from_bytes(&msg) {
                    Ok(_) => Ok(Some(MQTTMessage::PubComp(pubcompmsg))),
                    Err(err) => {
                        slog::warn!(self.logger, "PUBCOMP decode: {}", err);
                        Err(err)
                    }
                }
            } else if msgtype == MQTTMessageType::Disconnect as u8 {
                Ok(Some(MQTTMessage::Disconnect(MQTTMessageDisconnect {})))
            } else {
                slog::warn!(self.logger, "Decoder: Unknown msg received");
                Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Unknown msg received",
                ))
            }
        } else {
            Ok(None)
        }
    }

    // Find the next line in buf when there will be no more data coming.
    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(buf)
    }
}
