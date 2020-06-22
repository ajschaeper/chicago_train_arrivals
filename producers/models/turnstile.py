"""Creates a turnstile data producer"""
import logging
import json
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)

with open(f"{Path(__file__).parents[0]}/../../conf.json", "r") as fd:
    conf = json.load(fd)

class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load( f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        super().__init__(
            f"cta_te_{station_name}", 
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema, 
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        self.producer.producer(
                topic=self.topic,
                key={"timestamp": self.time_millis()},
                key_schema=Trunstile.key_schema,
                value_schema=Trunstile.value_schema,
                value={
                    "station_id": self.station.station_id,
                    "station_name": self.station.name,
                    "line:": self.station.color
                }
        )

