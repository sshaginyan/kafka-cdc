const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();

const kafka = new Kafka({
  clientId: 'crowdstrike',
  brokers: process.env.KAFKA_URL.replace(/kafka\+ssl:\/\//g, '').split(','),
  ssl: {
    rejectUnauthorized: false,
    ca: [process.env.KAFKA_TRUSTED_CERT],
    key: process.env.KAFKA_CLIENT_CERT_KEY,
    cert: process.env.KAFKA_CLIENT_CERT
  }
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'crowdstrike' });

(async () => {
    await producer.connect();
    
    setInterval(async () => {
        await producer.send({
            topic: 'crowdstrike',
            messages: [
                { value: JSON.stringify({ one: 1 }) }
            ]
        });
    }, 4000);
})();

(async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'crowdstrike' });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      console.log(data);
    }
  })
})();

app.listen(process.env.PORT || 8080);