import faust
from typing import Optional

app = faust.App('order_app', broker='kafka://localhost:9092')

class Order(faust.Record, serializer='json'):
    account_id:str
    status: Optional[str] = None  # Опциональный параметр, по умолчанию None

# Топик для входящих заказов
orders_topic = app.topic('orders', value_type=Order)

# Создаем канал для передачи сообщений между агентами
channel = app.channel(value_type=Order)

# Таблица для хранения количества заказов на каждый account_id
order_counts = app.Table('order_counts', key_type= str,value_type=int,
                         partitions=1,default=int)

# Агент для обработки заказов
@app.agent(orders_topic)
async def process_orders(orders):
    async for order in orders:
        account_id = order.account_id  # Получаем account_id из объекта Order
        order_counts[account_id] += 1   # Увеличиваем счетчик заказов для аккаунта
        print(f"Account {account_id} has {order_counts[account_id]} orders")
        print(order_counts.as_ansitable(title="order_counts"))  # Печатаем таблицу заказов
        ######
        if account_id==1:
            await channel.send(value=Order(account_id=account_id, status="from 1"))

# Второй агент, читающий сообщения из канала
@app.agent(channel)
async def receiver_agent(messages):
    async for message in messages:
        print(f"Receiver received: {message}")

