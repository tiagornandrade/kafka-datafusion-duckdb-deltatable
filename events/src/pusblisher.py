from kafka import KafkaProducer
import json
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Publisher:
    def __init__(self, topic_name):
        self.topic_name = topic_name
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Publisher initialized for topic '{self.topic_name}'")

    def publish(self, data):
        self.producer.send(topic=self.topic_name, value=data)
        self.producer.flush()
        logger.info(f"Published message to topic '{self.topic_name}': {data}")

    def close(self):
        self.producer.close()
        logger.info("Producer closed")
