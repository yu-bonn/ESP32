from confluent_kafka import Consumer, Producer, KafkaException
from collections import deque
import asyncio
from datetime import datetime, timedelta


# Apache Kafka のパラメータ設定
HOST = '150.65.230.59:9092'
TOPIC_SCD41_co2 = 'i483-sensors-s2410139-SCD41-co2'
TOPIC_SCD41_temperature = 'i483-sensors-s2410139-SCD41-temperature'
TOPIC_SCD41_humidity = 'i483-sensors-s2410139-SCD41-humidity'
TOPIC_BMP180_temperature = 'i483-sensors-s2410139-BMP180-temperature'
TOPIC_BMP180_air_pressure = 'i483-sensors-s2410139-BMP180-air_pressure'
topics = [TOPIC_SCD41_co2, TOPIC_SCD41_temperature, TOPIC_SCD41_humidity, TOPIC_BMP180_temperature, TOPIC_BMP180_air_pressure]
GROUP = 'consumer1'

TOPIC_SCD41_co2_crossed = 'i483-s2410139-co2_threshold-crossed'
TOPIC_BMP180_avg_temperature = ' i483-s2410139-BMP180_avg-temperature'

#トピックごとにqueue
topics_deques = {
    TOPIC_SCD41_co2 : deque(),
    TOPIC_BMP180_temperature : deque()
}

# Apache Kafka の接続設定
conf = {
    'bootstrap.servers': HOST
    , 'client.id': 's24010139'
    , 'group.id': GROUP
    , 'auto.offset.reset': 'smallest'
}

# Apache Kafka の接続設定（プロデューサー用）
producer_conf = {
    'bootstrap.servers': HOST,
    'client.id': 's24010139'
}

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

async def kafka_consume(conf, topics):
    # Apache Kafka へ Consume
    kafka = Consumer(conf)
    time_30 = 0
    try:
        kafka.subscribe(topics)
        while True:
            msg = kafka.poll(timeout = 1.0)
            if msg is None:
                await asyncio.sleep(1)
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            print('Received message: {}'.format(msg.value().decode('utf-8')))
            if msg.topic() == TOPIC_SCD41_co2:
                 topics_deques[msg.topic()].append(float(msg.value().decode('utf-8')))
                 await parse(float(msg.value().decode('utf-8')), msg.topic(), TOPIC_SCD41_co2_crossed, time_30)
            elif msg.topic() == TOPIC_BMP180_temperature:
                 topics_deques[msg.topic()].append(float(msg.value().decode('utf-8')))                 
                 await parse(float(msg.value().decode('utf-8')), msg.topic(), TOPIC_BMP180_avg_temperature, time_30)
                 if time_30 > 2:
                     time_30 = 0
                 time_30 += 1
    finally:
        kafka.close()

async def kafka_produce(conf, topics, msg):
    # Apache Kafka へ Publish
    kafka = Producer(conf)
    try:
        kafka.produce(topic = topics, value = msg)
    finally:
        kafka.flush()


async def parse(value, topic, p_topic, time_30):
    if topic == TOPIC_SCD41_co2:
        if value >= 700:
            kafka_produce(producer_conf, p_topic, "yes")
            print('yes')
        else:
            kafka_produce(producer_conf, p_topic, "no")
            print('no')
    if topic == TOPIC_BMP180_temperature:
        if len(topics_deques[TOPIC_BMP180_temperature]) > 20:
            topics_deques[TOPIC_BMP180_temperature].popleft()
        if time_30 > 2:
            last_five_elements = list(topics_deques[TOPIC_BMP180_temperature])[-20:]
            last_five_elements = [float(element) for element in last_five_elements if isinstance(element, (int, float))]
            if last_five_elements:
                average = sum(last_five_elements) / len(last_five_elements)
                average = round(average, 2)  # 小数点第2位までに丸める
                await kafka_produce(producer_conf, p_topic, str(average))
                print(average)
            else:
                print("No data available to calculate average")
    return None
        

asyncio.run(kafka_consume(conf, topics))