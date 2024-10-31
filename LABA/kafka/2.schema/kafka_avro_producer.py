import os
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

topic = "my-avro-topic"
schema = "user.avsc"

class User(object):
    def __init__(self, name, address, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color
        # address should not be serialized, see user_to_dict()
        self._address = address


def user_to_dict(user, ctx):
    # User._address must not be serialized; omit from dict
    return dict(name=user.name,
                favorite_number=user.favorite_number,
                favorite_color=user.favorite_color)


def delivery_report(err, msg):
 
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

##----------------------------------------
##  ЧИТАЕМ СХЕМУ ИЗ ФАЙЛА
try:
    path = os.path.realpath(os.path.dirname(__file__))
    print(f"{path}/{schema}")
    with open(f"{path}/{schema}") as f:
        schema_str = f.read()
except FileNotFoundError:
    print(f"Файл '{schema}' не найден.")
except Exception as e:
    print(f"Произошла ошибка: {e}")


schema_registry_conf = {'url': "http://localhost:8081/"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

##
# AvroSerializer автоматически регистрирует схему в Confluent Schema Registry
#  При первом использовании схемы, она отправляется в Schema Registry, 
# где ей присваивается уникальный идентификатор (ID). 
# Этот идентификатор затем ассоциируется с данными, отправленными в Kafka, 
# вместо того чтобы каждый раз отправлять всю схему. 
# Это оптимизирует передачу данных и гарантирует согласованность схем.
avro_serializer = AvroSerializer(schema_registry_client,
                                    schema_str,
                                    user_to_dict)

string_serializer = StringSerializer('utf_8')

producer_conf = {'bootstrap.servers': 'localhost:9092'}

producer = Producer(producer_conf)

print("Producing user records to topic {}. ^C to exit.".format(topic))
for _ in range(4):
    # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        user_name = "qwe"
        user_address = "add1"
        user_favorite_number = 1 
        user_favorite_color = "red "
        user = User(name=user_name,
                    address=user_address,
                    favorite_color=user_favorite_color,
                    favorite_number=user_favorite_number)
        ##################
        # SerializationContext — это объект, который может быть передан в функции сериализации,
        #  такие как AvroSerializer. Он содержит дополнительную информацию о контексте 
        # сериализации, такую как топик Kafka, в который отправляются данные, 
        # и другие параметры, полезные при сериализации.
        producer.produce(topic=topic,
                            key=string_serializer(str(uuid4())),
                            value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
    except KeyboardInterrupt:
        break
    except ValueError:
        print("Invalid input, discarding record...")
        continue

print("\nFlushing records...")
producer.flush()