"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
from pathlib import Path

import requests


logger = logging.getLogger(__name__)


with open(f"{Path(__file__).parents[0]}/../conf.json", "r") as fd:
    conf = json.load(fd)


KAFKA_CONNECT_URL = conf["kafka"]["connect"]["url"]
SCHEMA_REGISTRY_URL = conf["schema_registry"]["url"]
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "key.converter.schema.registry.url": SCHEMA_REGISTRY_URL,
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "value.converter.schema.registry.url": SCHEMA_REGISTRY_URL,
                "batch.max.rows": "500",
                "connection.url": "jdbc:postgresql://localhost:5432/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": "cta.raw.",
                "poll.interval.ms": "604800000",
            }
        }),
    )

    ## Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
