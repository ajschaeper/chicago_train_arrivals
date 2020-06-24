"""Defines trends calculations for stations"""
import logging
import json
from pathlib import Path
from dataclasses import dataclass

import faust


logger = logging.getLogger(__name__)

with open(f"{Path(__file__).parents[0]}/../conf.json", "r") as fd:
    conf = json.load(fd)

KAFKA_BROKER_URL = conf["kafka"]["broker"]["url"]
TOPIC_NAME_INBOUND = "cta.raw.stations"
TOPIC_NAME_OUTBOUND = "cta.stations"

# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# init Faust app
app = faust.App("cta.streams.stations", broker=f"kafka://{KAFKA_BROKER_URL}", store="memory://")

# define topics for inbound and outbound messages
inb_topic = app.topic(TOPIC_NAME_INBOUND, value_type=Station)
otb_topic = app.topic(TOPIC_NAME_OUTBOUND, partitions=1)

# define table to make conversion
table = app.Table(
    "cta.stations.tbl",
    default=TransformedStation,
    partitions=1,
    changelog_topic=otb_topic,
)

# upon inbound message, update table and trigger outbound message
@app.agent(inb_topic)
async def in_and_out(stations):
    
    # for each station event in scope (e.g. line color is not None)
    async for st in stations.filter(lambda s: s.blue == True or s.red == True or s.green == True):

        print(type(st))
        print(st)

        # populate table to yield output event
        table[st.station_id] = TransformedStation(
            station_id   = st.station_id,
            station_name = st.station_name,
            order        = st.order,
            line         =  "blue" if st.blue == True else \
                            "red" if st.red == True else \
                            "green" if st.green == True else \
                            "__error_unknown_line__"
        )
        print(f"Transformed station: {st.station_id}")

if __name__ == "__main__":
    app.main()
