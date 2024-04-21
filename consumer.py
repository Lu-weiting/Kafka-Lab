import datetime
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

consumer = KafkaConsumer('topic-test', bootstrap_servers='localhost:9092')
print("已啟動。用SIGINT (CTRL+C) 中止程式")
try:
    for msg in consumer:
        msg: ConsumerRecord = msg  # for type hint
        print(
                "timestamp:", datetime.datetime.fromtimestamp(msg.timestamp / 1000),
                "value:", msg.value,
                "offset:", msg.offset,
                "partition:", msg.partition
                )
except KeyboardInterrupt:
    consumer.close()