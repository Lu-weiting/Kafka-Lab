import datetime
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

consumer = KafkaConsumer(
        'topic-test',
        bootstrap_servers='localhost:9092',
        auto_offset_reset="earliest",
        group_id='test-group'
        # client_id='example-consumer',
)
print("已啟動。用SIGINT (CTRL+C) 中止程式")
try:
    for msg in consumer:
        msg: ConsumerRecord = msg  # for type hint
        print({
            "partition": msg.partition,
            "offset": msg.offset,
            "value": msg.value.decode()
            })
        prefix = f"{msg.topic}[{msg.partition} | {msg.offset}] / {datetime.datetime.fromtimestamp(msg.timestamp / 1000)}"
        print(f"- {prefix} {msg.value.decode()}")
        
except KeyboardInterrupt:
    consumer.commit()
    consumer.close()