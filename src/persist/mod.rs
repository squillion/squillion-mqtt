use crate::BrokerId;

pub fn get_schema(broker_id: &BrokerId) -> std::string::String {
    format!(
        "mqtt__{}__{}",
        broker_id.tenant_id.clone().unwrap_or(String::from("''")),
        broker_id.broker_id
    )
}
