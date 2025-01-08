use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::io::Write;
use std::time::Duration;
use tracing::trace;
use tracing_subscriber::fmt::MakeWriter;

pub struct SimpleKafkaAppender {
    producer: BaseProducer,
    topic: String,
}
pub struct KafkaWriter<'a> {
    producer: &'a BaseProducer,
    topic: &'a str,
}

impl SimpleKafkaAppender {
    pub fn new(producer: BaseProducer, topic: String) -> SimpleKafkaAppender {
        Self { producer, topic }
    }
}
impl Write for SimpleKafkaAppender {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Ok(0)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Write for KafkaWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let payload = BaseRecord::to(self.topic).key(&()).payload(buf);
        match self.producer.send(payload) {
            Ok(_) => {
                if let Err(e) = self.producer.flush(Duration::from_millis(1000)){
                    trace!("flush message error: {}", e);
                }
                trace!("Sent message to topic: {}", self.topic);
            }
            Err(e) => {
                trace!(
                    "Failed to send message to topic: {}, error: {}",
                    self.topic,
                    e.0
                );
            }
        }
        Ok(0)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        trace!("flushing buffer");
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SimpleKafkaAppender {
    type Writer = KafkaWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        KafkaWriter {
            producer: &self.producer,
            topic: &self.topic,
        }
    }
}
