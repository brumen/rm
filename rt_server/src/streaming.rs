
pub trait Streaming {
    fn kafka_server_name(&self) -> String;
    fn kafka_port(&self) -> i32;
}
