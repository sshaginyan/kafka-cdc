const jsforce = require('jsforce');
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

const conn = new jsforce.Connection();
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'crowdstrike' });

(async () => {
    await conn.login('heroku_pe_dev@herokudev.org', 'khCaOSvT#4eI1XC' + 'eP3QHn2n511AVGTLnHVn81IYC');

    const cdcStream = conn.streaming.createClient([
      new jsforce.StreamingExtension.Replay('/data/ChangeEvents', -1),
      new jsforce.StreamingExtension.AuthFailure(() => process.exit(1))
    ]);
    await producer.connect();
    
    setInterval(async () => {
      cdcStream.subscribe('/data/ChangeEvents', async data => {  
        console.log('PRODUCED======', data);
        await producer.send({
            topic: 'crowdstrike',
            messages: [
                { value: JSON.stringify(data) }
            ]
        });
      });
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