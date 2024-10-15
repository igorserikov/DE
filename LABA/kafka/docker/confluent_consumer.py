from confluent_kafka import Consumer, KafkaError

# Настройка consumer
conf = {
    'bootstrap.servers': 'localhost:9092',  # адрес вашего Kafka брокера
    'group.id': 'my-group1',                   # ID вашей consumer group
    'auto.offset.reset': 'earliest'           # чтение с начала, если смещения не найдены
}

# Создание consumer
consumer = Consumer(conf)

# Подписка на тему
consumer.subscribe(['test-topic'])

try:
    while True:
        # Чтение сообщения
        msg = consumer.poll(1.0)  # тайм-аут в 1 секунду

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Достигнут конец партиции
                print('End of partition event')
            else:
                print(f"Error: {msg.error()}")
        else:
            # Обработка полученного сообщения
            key = msg.key().decode('utf-8') if msg.key() else None
            value = msg.value().decode('utf-8') if msg.value() else None
            print(f"Received message: key={key}, value={value}, offset={msg.offset()}")
except KeyboardInterrupt:
    pass
finally:
    # Закрытие consumer
    consumer.close()