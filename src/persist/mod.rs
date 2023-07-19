use crate::BrokerId;

pub fn get_schema(broker_id: &BrokerId) -> std::string::String {
    format!("mqtt__{}__{}", broker_id.tenant_id, broker_id.broker_id)
}
