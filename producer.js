const faker = require('faker');
const { Kafka } = require('kafkajs');

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

(async () => {
    await producer.connect();
    
    setInterval(async () => {
        const data = { name: faker.name.findName(), email: faker.internet.email(), phone: faker.phone.phoneNumber() };
        await producer.send({
            topic: 'crowdstrike',
            messages: [
                { value: JSON.stringify(data) }
            ]
        });
    }, 2000);
})();

require('http').createServer((req, res) => { res.end('ok'); }).listen(process.env.PORT);