const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'example-producer',
  brokers: ['localhost:9092']
});


const consumer = kafka.consumer({ groupId: 'test-group' });
const consume = async () => {
    
  await consumer.connect();
  await consumer.subscribe({ topic: 'topic-test', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
    },
  });
};

consume().catch((err) => {
  console.error("Error in consumer: " + err.message);
});
