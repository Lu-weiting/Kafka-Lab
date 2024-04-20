const { Kafka, CompressionTypes } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'example-producer',
    brokers: ['localhost:9092']
});
const createTopic = async (topic) => {
    const admin = kafka.admin();
    await admin.connect();
    const topics = await admin.listTopics();
    if (!topics.includes(topic)) {
        await admin.createTopics({
            topics: [{
                topic: topic,
                numPartitions: 2
            }],
        })
        const fetchTopicOffsets = await admin.fetchTopicOffsets(topic)
        console.log(`Crate topic:${topic} successful!`)
        console.log(fetchTopicOffsets)
    }
    await admin.disconnect();
}

const producer = kafka.producer();

const sendMessage = async () => {
    try {
        const getRandomNumber = () => Math.round(Math.random(10) * 1000);
        const data = await producer
            .send({
                topic,
                compression: CompressionTypes.GZIP,
                messages: [
                    { 
                        key: `${getRandomNumber()}`,
                        value: `Hello Kafka from Node.js!, ${getRandomNumber()}`
                    },
                ]
            });
        return console.log(data);
    } catch (e) {
        return console.error(`Could not write message`, e);
    }
}
const topic = 'topic-test';
const produce = async () => {
    await createTopic(topic);
    await producer.connect()
    setInterval(sendMessage, 2000)
};
produce().catch(e => console.error(`[example/producer] ${e.message}`, e));

