import { Kafka, logLevel } from 'kafkajs';
import { ulid } from 'ulid';
import { parseHeaders } from './utils';

const TIMEOUT_DURATION = 10_000; // 10 seconds
const NUM_MESSAGES_TO_SEND = 3;

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'some-producer',
  logLevel: logLevel.NOTHING,
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'some-producer' });

const run = async () => {
  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({ topic: 'sum-replies', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const headers = parseHeaders(message.headers);
      const value = message.value?.toString();
      console.log('received message', value, headers);

      // headers validation
      const { correlationId } = headers;
      if (!correlationId) {
        throw new Error('missing correlationId in headers');
      }
    },
  });

  for (let i = 0; i < NUM_MESSAGES_TO_SEND; i++) {
    const correlationId = ulid();

    const recordMetadata = await producer.send({
      topic: 'sum-requests',
      messages: [
        {
          headers: { correlationId, replyTo: 'sum-replies' },
          value: 'Hello, World!',
        },
      ],
    });

    console.log('recordMetadata', recordMetadata);
  }

  // set a timeout to disconnect both producer and consumer
  setTimeout(() => {
    console.log('disconnecting producer and consumer');

    producer
      .disconnect()
      .then(() => {
        console.log('disconnected producer');
      })
      .catch(console.error);

    consumer
      .disconnect()
      .then(() => {
        console.log('disconnected consumer');
      })
      .catch(console.error);
  }, TIMEOUT_DURATION);
};

run().catch(console.error);
