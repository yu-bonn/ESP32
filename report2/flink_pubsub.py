from typing import Iterable
import json

from pyflink.common import Time, Configuration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, AggregateFunction
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.window import TimeWindow

# consume
TOPIC_SCD41_co2 = 'i483-sensors-s2410139-SCD41-co2'
TOPIC_SCD41_temperature = 'i483-sensors-s2410139-SCD41-temperature'
TOPIC_SCD41_humidity = 'i483-sensors-s2410139-SCD41-humidity'
TOPIC_BMP180_temperature = 'i483-sensors-s2410139-BMP180-temperature'
TOPIC_BMP180_air_pressure = 'i483-sensors-s2410139-BMP180-air_pressure'

# produce
TOPIC_SCD41_min_co2 = 'i483-sensors-s2410139-analytics-SCD41_min-co2'
TOPIC_SCD41_max_co2 = 'i483-sensors-s2410139-analytics-SCD41_max-co2'
TOPIC_SCD41_ave_co2 = 'i483-sensors-s2410139-analytics-SCD41_ave-co2'
TOPIC_SCD41_min_temperature = 'i483-sensors-s2410139-analytics-SCD41_min-temperature'
TOPIC_SCD41_max_temperature = 'i483-sensors-s2410139-analytics-SCD41_max-temperature'
TOPIC_SCD41_ave_temperature = 'i483-sensors-s2410139-analytics-SCD41_ave-temperature'
TOPIC_SCD41_min_humidity = 'i483-sensors-s2410139-analytics-SCD41_min-humidity'
TOPIC_SCD41_max_humidity = 'i483-sensors-s2410139-analytics-SCD41_max-humidity'
TOPIC_SCD41_ave_humidity = 'i483-sensors-s2410139-analytics-SCD41_ave-humidity'
TOPIC_BMP180_min_temperature = 'i483-sensors-s2410139-analytics-BMP180_min-temperature'
TOPIC_BMP180_max_temperature = 'i483-sensors-s2410139-analytics-BMP180_max-temperature'
TOPIC_BMP180_ave_temperature = 'i483-sensors-s2410139-analytics-BMP180_ave-temperature'
TOPIC_BMP180_min_air_pressure = 'i483-sensors-s2410139-analytics-BMP180_min-air_pressure'
TOPIC_BMP180_max_air_pressure = 'i483-sensors-s2410139-analytics-BMP180_max-air_pressure'
TOPIC_BMP180_ave_air_pressure = 'i483-sensors-s2410139-analytics-BMP180_ave-air_pressure'

input_topics = [
    TOPIC_SCD41_co2, TOPIC_SCD41_temperature, TOPIC_SCD41_humidity, 
    TOPIC_BMP180_temperature, TOPIC_BMP180_air_pressure
]

output_topics = {
    TOPIC_SCD41_co2: [TOPIC_SCD41_min_co2, TOPIC_SCD41_max_co2, TOPIC_SCD41_ave_co2],
    TOPIC_SCD41_temperature: [TOPIC_SCD41_min_temperature, TOPIC_SCD41_max_temperature, TOPIC_SCD41_ave_temperature],
    TOPIC_SCD41_humidity: [TOPIC_SCD41_min_humidity, TOPIC_SCD41_max_humidity, TOPIC_SCD41_ave_humidity],
    TOPIC_BMP180_temperature: [TOPIC_BMP180_min_temperature, TOPIC_BMP180_max_temperature, TOPIC_BMP180_ave_temperature],
    TOPIC_BMP180_air_pressure: [TOPIC_BMP180_min_air_pressure, TOPIC_BMP180_max_air_pressure, TOPIC_BMP180_ave_air_pressure]
}

kafka_props = {
    'bootstrap.servers': '150.65.230.59:9092',
    'group.id': 'consumer1'
}

class StatsAggregateFunction(AggregateFunction):

    def create_accumulator(self):
        return (0.0, float('inf'), float('-inf'), 0, 0.0)  # (sum, min, max, count, avg)

    def add(self, value, accumulator):
        sum_val, min_val, max_val, count, _ = accumulator
        sum_val += value
        min_val = min(min_val, value)
        max_val = max(max_val, value)
        count += 1
        avg_val = sum_val / count if count != 0 else 0.0
        return (sum_val, min_val, max_val, count, avg_val)

    def get_result(self, accumulator):
        return {'min': accumulator[1], 'max': accumulator[2], 'avg': accumulator[4]}

    def merge(self, a, b):
        sum_val = a[0] + b[0]
        min_val = min(a[1], b[1])
        max_val = max(a[2], b[2])
        count = a[3] + b[3]
        avg_val = sum_val / count if count != 0 else 0.0
        return (sum_val, min_val, max_val, count, avg_val)

class JsonSerializeMapFunction(MapFunction):
    def map(self, value):
        return json.dumps(value)

class ValidateAndExtractMapFunction(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)
            if 'topic' in data and 'timestamp' in data and 'value' in data:
                return data['topic'], data['timestamp'], float(data['value'])
            else:
                raise ValueError('Missing required fields')
        except (ValueError, TypeError, json.JSONDecodeError) as e:
            return 'error_topic', None, None

def process_kafka_data(input_topics, output_topics, kafka_props):
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Kafka connector JAR
    #jar_path = "file:///C:/ESP32/ESP32/flink_kafka/flink-connector-kafka_2.12-1.14.0.jar"  # Correct path to the Kafka connector JAR
    #env.add_jars(jar_path)

    # Set pipeline configuration
    config = Configuration()
    #config.set_string("pipeline.jars", jar_path)
    env.configure(config)

    for input_topic in input_topics:
        kafka_consumer = FlinkKafkaConsumer(
            topics=[input_topic],
            deserialization_schema=SimpleStringSchema(),
            properties=kafka_props
        )

        kafka_producers = [
            FlinkKafkaProducer(
                topic=output_topic,
                serialization_schema=SimpleStringSchema(),
                producer_config=kafka_props
            ) for output_topic in output_topics[input_topic]
        ]

        error_producer = FlinkKafkaProducer(
            topic='error_topic',
            serialization_schema=SimpleStringSchema(),
            producer_config=kafka_props
        )

        stream = env.add_source(kafka_consumer)

        # Assign timestamps and watermarks
        stream = stream.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps()
        )

        parsed_stream = stream.map(ValidateAndExtractMapFunction())

        valid_stream = parsed_stream.filter(lambda x: x[0] != 'error_topic')
        error_stream = parsed_stream.filter(lambda x: x[0] == 'error_topic')

        values = valid_stream.map(lambda x: x[2])

        windowed_stream = values.window_all(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))

        stats = windowed_stream.aggregate(StatsAggregateFunction())

        stats_json = stats.map(JsonSerializeMapFunction())

        for producer in kafka_producers:
            stats_json.add_sink(producer)

        error_stream.map(lambda x: json.dumps({'error': 'Invalid data', 'data': x})).add_sink(error_producer)

    env.execute('Kafka Multiple Topics Stats Example')

if __name__ == '__main__':
    process_kafka_data(input_topics, output_topics, kafka_props)
