"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)

with open(f"{Path(__file__).parents[0]}/../../conf.json", "r") as fd:
    conf = json.load(fd)

REST_PROXY_URL = conf["kafka"]["rest_proxy"]["url"]

WEATHER_TOPIC_NAME = "cta_weather" 


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )
    
    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        super().__init__(
            WEATHER_TOPIC_NAME,
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        data_key = {"timestamp": self.time_millis()}
        
        data_value = {
            "temperature": self.temp,
            "status": self.status.name
        }

        data = {
            "key_schema": json.dumps(Weather.key_schema, indent=4),
            "value_schema": json.dumps(Weather.value_schema, indent=4), 
            "records": [ {"key": data_key, "value": data_value} ]
        }

        headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}
        
        resp = requests.post(
            f"{REST_PROXY_URL}/topics/{WEATHER_TOPIC_NAME}",
            data=json.dumps(data),
            headers=headers,
        )

        try:
            resp.raise_for_status()
        except:
            print(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=4)}")

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
