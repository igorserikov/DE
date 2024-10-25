##  https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_consumer.py

import argparse
import os

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


class User(object):

    def __init__(self, name=None, favorite_number=None, favorite_color=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color


def dict_to_user(obj, ctx):
 
    if obj is None:
        return None

    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])




topic = "my-avro-topic"
schema = "user.avsc"

try:
    path = os.path.realpath(os.path.dirname(__file__))
    print(f"{path}/{schema}")
    with open(f"{path}/{schema}") as f:
        schema_str = f.read()
except FileNotFoundError:
    print(f"Файл '{schema}' не найден.")
except Exception as e:
    print(f"Произошла ошибка: {e}")

sr_conf = {'url': "http://localhost:8081/"}
schema_registry_client = SchemaRegistryClient(sr_conf)

avro_deserializer = AvroDeserializer(schema_registry_client,
                                        schema_str,
                                        dict_to_user)

consumer_conf = {'bootstrap.servers': 'localhost:9092',
                    'group.id': "my_group1",
                    'auto.offset.reset': "earliest"}

consumer = Consumer(consumer_conf)
consumer.subscribe([topic])

for _ in range(4):
    try:
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        if user is not None:
            print("User record {}: \n,name: {}\n"
                    "\tfavorite_number: {}\n"
                    "\tfavorite_color: {}\n"
                    .format(msg.key(), user.name,
                            user.favorite_number,
                            user.favorite_color))
    except KeyboardInterrupt:
        break

consumer.close()