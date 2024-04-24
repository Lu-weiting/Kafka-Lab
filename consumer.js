const { Kafka, logLevel } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'example-producer',
  brokers: ['localhost:9092']
});


const consumer = kafka.consumer({ groupId: 'test-group'});
const consume = async () => {
    
  await consumer.connect();
  await consumer.subscribe({ topic: 'topic-test', fromBeginning: true});

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} #${message.value}`);
    },
  });
  
};

consume().catch((err) => {
  console.error("Error in consumer: " + err.message);
});
const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect();
      await consumer.commitOffsets();
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
      await consumer.commitOffsets();
    } finally {
      process.kill(process.pid, type)
    }
  })
})