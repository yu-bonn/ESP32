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

TOPIC_SCD41_co2_crossed = 'i483-sensors-s2410139-co2_threshold-crossed'
TOPIC_BMP180_avg_temperature = 'i483-sensors-s2410139-BMP180_avg-temperature'

# トピックごとにqueue
topics_deques = {
    TOPIC_SCD41_co2: deque(),
    TOPIC_BMP180_temperature: deque()
}

# Apache Kafka の接続設定
conf = {
    'bootstrap.servers': HOST,
    'client.id': 's24010139',
    'group.id': GROUP,
    'auto.offset.reset': 'latest' 
}

# Apache Kafka の接続設定（プロデューサー用）
producer_conf = {
    'bootstrap.servers': HOST,
    'client.id': 's24010139'
}

def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

async def kafka_produce(conf, topic, msg):
    # Apache Kafka へ Publish
    kafka = Producer(conf)
    try:
        kafka.produce(topic=topic, value=msg, callback=delivery_callback)
        kafka.flush()  # メッセージを送信するためにフラッシュする
    except KafkaException as e:
        print(f"Exception occurred: {e}")
    finally:
        pass  # with ステートメントで自動的にクリーンアップされるため、ここでのクローズは不要

pre_value = "yes"
# データの計算
async def parse(value, topic, p_topic, time_30):
    global pre_value  # グローバル変数として宣言
    if topic == TOPIC_SCD41_co2:
        if value >= 700:
            if pre_value == "no":
                await kafka_produce(producer_conf, p_topic, "yes")
                print('yes')
                pre_value = "yes"
        else:
            if pre_value == "yes":
                await kafka_produce(producer_conf, p_topic, "no")
                print('no')
                pre_value = "no"
    if topic == TOPIC_BMP180_temperature:
        if len(topics_deques[TOPIC_BMP180_temperature]) > 20:
            topics_deques[TOPIC_BMP180_temperature].popleft()
        if time_30 == 2:
            last_20_elements = list(topics_deques[TOPIC_BMP180_temperature])[-20:]
            last_20_elements = [float(element) for element in last_20_elements if isinstance(element, (int, float))]
            if last_20_elements:
                average = sum(last_20_elements) / len(last_20_elements)
                average = round(average, 2)
                print(f"Average temperature: {average}")
                await kafka_produce(producer_conf, p_topic, str(average))
            else:
                print("No data available to calculate average")
    return None

async def kafka_consume(conf, topics):
    kafka = Consumer(conf)
    time_30 = 0
    try:
        kafka.subscribe(topics)
        while True:
            msg = kafka.poll(timeout=1.0)
            if msg is None:
                await asyncio.sleep(1)
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            if msg.topic() == TOPIC_SCD41_co2:
                print('Received message: {}'.format(msg.value().decode('utf-8')))
                topics_deques[msg.topic()].append(float(msg.value().decode('utf-8')))
                await parse(float(msg.value().decode('utf-8')), msg.topic(), TOPIC_SCD41_co2_crossed, time_30)
                print('produce1')
            elif msg.topic() == TOPIC_BMP180_temperature:
                if time_30 == 2:
                    time_30 = 0
                time_30 += 1
                print('Received message: {}'.format(msg.value().decode('utf-8')))
                topics_deques[msg.topic()].append(float(msg.value().decode('utf-8')))
                await parse(float(msg.value().decode('utf-8')), msg.topic(), TOPIC_BMP180_avg_temperature, time_30)
                print('produce2')
                
    finally:
        kafka.close()

asyncio.run(kafka_consume(conf, topics))
