process.env.DATABASE_URL += '?ssl=true';

const jsforce = require('jsforce');
const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
const server = require('http').Server(app);
const io = require('socket.io')(server);
const knex = require('knex')({
  client: 'postgres',
  connection: process.env.DATABASE_URL
});

const {
  SF_PASS,
  SF_USER,
  KAFKA_TOPIC_CDC
} = process.env;

app.use(express.static('public'));

const connections = [];

io.on('connection', async socket => {
  connections.push(socket);
  socket.on('disconnect', function() {
      console.log('Got disconnect!');
      const index = connections.indexOf(socket);
      connections.splice(index, 1);
   });
});

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
const consumer = kafka.consumer({ groupId: KAFKA_TOPIC_CDC });

(async () => {
  await conn.login(SF_USER, SF_PASS);
      const cdcStream = conn.streaming.createClient([
      new jsforce.StreamingExtension.Replay('/data/ChangeEvents', -1),
      new jsforce.StreamingExtension.AuthFailure(() => process.exit(1))
    ]);
    await producer.connect();
    cdcStream.subscribe('/data/ChangeEvents', async data => {
      await producer.send({
          topic: KAFKA_TOPIC_CDC,
          messages: [
              { value: JSON.stringify(data) }
          ]
      });
    });
})();

(async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC_CDC });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      connections.forEach( async socket => {
        socket.emit('message', data);
        const dataString = JSON.stringify(data);
        const dataClone = JSON.parse(dataString);
        delete dataClone.payload.ChangeEventHeader;
        await knex.withSchema('public').table('accounts').insert({ changes: JSON.stringify(dataClone.payload) });
      });
    }
  })
})();

server.listen(process.env.PORT || 8080);