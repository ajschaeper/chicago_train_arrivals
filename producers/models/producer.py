"""Producer base-class providing common utilites and functionality"""
import logging
import time
import json
from pathlib import Path

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

with open(f"{Path(__file__).parents[0]}/../../conf.json") as fd:
    conf = json.load(fd)

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.broker_properties = {
            "bootstrap.servers": conf["kafka"]["broker"]["url"],
            "compression.type": conf["kafka"]["producer"]["compression.type"],
            "client.id": conf["kafka"]["producer"]["client.id"],
            # "batch.size" 
            # "linger.ms" 
            # "acks"
            # "retries"
        }

        self.schema_registry = CachedSchemaRegistryClient(conf["schema_registry"]["url"])

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry = self.schema_registry
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logger.info("""Automatic topic creation not supported 
            Please follow these steps to safely create your topics:
                1. cd $BASE_DIR
                2. vim conf.json #define your topics in the config
                3. python create_topics.py #create new topics, exitsting are left untouched""")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer close incomplete - skipping")

    # TODO function looks like duplicate
    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
