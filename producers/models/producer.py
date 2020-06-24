"""Producer base-class providing common utilites and functionality"""
import logging
import time
import json
from pathlib import Path

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


with open(f"{Path(__file__).parents[0]}/../../conf.json", "r") as fd:
    conf = json.load(fd)

SCHEMA_REGISTRY_URL = conf["schema_registry"]["url"]
KAFKA_BROKER_URL = conf["kafka"]["broker"]["url"]
NUM_PARTITIONS = conf["kafka"]["broker"]["topics"]["default_config"]["num_partitions"]
NUM_REPLICAS = conf["kafka"]["broker"]["topics"]["default_config"]["replication_factor"]

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=NUM_PARTITIONS,
            num_replicas=NUM_REPLICAS,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": f"PLAINTEXT://{KAFKA_BROKER_URL}"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(self.broker_properties,
                                     schema_registry=CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL),
                                     default_key_schema=self.key_schema,
                                     default_value_schema=self.value_schema
                                     )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        AdminClient(self.broker_properties).create_topics([NewTopic(self.topic_name, num_partitions=NUM_PARTITIONS)])

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer is not None:
            self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
