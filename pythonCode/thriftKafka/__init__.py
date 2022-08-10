from kafka import KafkaConsumer
from thrift.protocol import TBinaryProtocol
from thrift import TSerialization
from thriftKafka.ttypes import Person
import logging

import traceback

# logger = logging.getLevelName()

# 将从kafka中读取的二进制流数据反序列化为python对象
def d_serialization(bytes):
    person = Person()
    thrift_ProtocolFactory = TBinaryProtocol.TBinaryProtocolFactory()
    TSerialization.deserialize(person, buf=bytes, protocol_factory=thrift_ProtocolFactory)
    return person


# 将python对象序列化为二进制流，用于存入kafka
def s_serialization(obj):
    thrift_ProtocolFactory = TBinaryProtocol.TBinaryProtocolFactory()
    bytes = TSerialization.serialize(thrift_object=obj, protocol_factory=thrift_ProtocolFactory)
    return bytes



class MyKafkaConsumer:
    def __init__(self, topic, group_id, d_serialization):
        self.topic = topic
        self.group_id = group_id
        self.d_serialization = d_serialization

    def pull(self):
        print("start pull...")
        consumer = KafkaConsumer(bootstrap_servers="192.168.134.128:9092",
                                 group_id=self.group_id,
                                 value_deserializer=d_serialization,
                                 auto_offset_reset = "earliest")
        print("start subcrib...")
        consumer.subscribe(self.topic)
        for msg in consumer:
            person = msg.value
            yield person


if __name__ == "__main__":
    consumer = MyKafkaConsumer(topic="kafka-thrift", group_id= "pg07",d_serialization = d_serialization)
    try:
        iterate = consumer.pull()
        for person in iterate:
            print(person)
    except Exception as e:
        traceback.print_exc()


