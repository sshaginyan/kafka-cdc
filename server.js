process.env.DATABASE_URL += '?ssl=true';

const faker = require('faker');
const uuidv1 = require('uuid/v1');
const { Kafka } = require('kafkajs');
const knex = require('knex')({
  client: 'postgres',
  connection: process.env.DATABASE_URL
});

const kafka = new Kafka({
  clientId: 'crowdstrike',
  brokers: ['ec2-18-209-198-121.compute-1.amazonaws.com:9096', 'ec2-3-211-8-44.compute-1.amazonaws.com:9096', 'ec2-3-209-193-170.compute-1.amazonaws.com:9096'],
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
        const data = {
          external_id__c: uuidv1(),
          name__c: faker.name.findName(),
          email__c: faker.internet.email(),
          phone__c: faker.phone.phoneNumber()
        };
        await producer.send({
            topic: 'crowdstrike',
            messages: [
                { value: JSON.stringify(data) }
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
      await knex.withSchema('salesforce').table('crowdstrike__c').insert(data);
    }
  })
})();

require('http').createServer((req, res) => { res.end('ok'); }).listen(process.env.PORT);