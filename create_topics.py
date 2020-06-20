"""initialize topics defined in conf.json"""
import json

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer

# load config 
with open("conf.json", "r") as fd:
    conf = json.load(fd)

BROKER_URL = conf["kafka"]["broker"]["url"]
TOPICS_TOBE = conf["kafka"]["broker"]["topics"].keys()

def topic_exists(client, topic_name):
    """Checks if the given topic exists"""
    topic_metadata = client.list_topics(timeout=5)
    return topic_metadata.topics.get(topic_name) is not None

def create_topic(client, topic_name):
    """Creates the topic with the given topic name"""
    futures = client.create_topics(
        [
            NewTopic(
                topic = topic_name,
                **conf["kafka"]["broker"]["topics"][topic_name]
            )
        ]
    )

    for topic, future in futures.items():
        try:
            future.result()
            print(f"topic created: {topic_name}")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")
            raise

def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    for topic_name in TOPICS_TOBE:
        exists = topic_exists(client, topic_name)
        print(f"topic {topic_name} exists: {exists}")

        if not exists: create_topic(client, topic_name)
        else: print(f"topic already exists, delete with: kafka-topics --zookeeper {conf['zookeeper']['url']} --topic {topic_name} --delete")

if __name__ == "__main__":
    main()

# eof
