from time import sleep
from typing import Iterable

from pyflink.common import WatermarkStrategy, Time, Configuration
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.datastream.connectors.number_seq import NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow, TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema,KafkaRecordSerializationSchemaBuilder,KafkaSourceBuilder, KafkaSource


class AggregateWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self,
                key:int,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple[int, int]]
                ) -> list[float]:
        cnt = 0
        for e in elements:
            cnt += e[1]
        print(f" element key/cnt: {key}, {cnt}")
        return [float(cnt)]

def state_access_demo():
    # 0. config
    config = Configuration().set_string("python.execution-mode", "thread")
    # 1. create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment(config)

    #ks:KafkaSource = KafkaSourceBuilder()\
    #    .set_topic_pattern("i483-sensors-gen-data-[a-zA-Z0-9]+")\
    #    .set_bootstrap_servers('150.65.230.59:9092')\
    #    .set_value_only_deserializer(SimpleStringSchema())\
    #    .build()

    ## 2. Setup a kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
            topics='i483-sensors-gen-data-increasing',
            deserialization_schema=SimpleStringSchema(),
            properties={'bootstrap.servers': '150.65.230.59:9092'}
            )
    ds = env.add_source(kafka_consumer)

    # 3. define the execution logic
    ds = ds.map(lambda a: (int(a) % 4, 1), output_type=Types.TUPLE([Types.LONG(), Types.LONG()]))
    ds = ds.key_by(lambda a: a[0])
    ds = ds.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    ds = ds.process(AggregateWindowProcessFunction(), Types.DOUBLE())

    _ = ds.print()
    # 4. create sink and emit result to sink
    # TODO sink back to kafka

    # 5. execute the job
    env.execute('state_access_demo')


if __name__ == '__main__':
    state_access_demo()

