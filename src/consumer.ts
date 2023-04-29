import { Kafka, logLevel } from 'kafkajs';
import { parseHeaders } from './utils';

const TIMEOUT_DURATION = 10_000; // 10 seconds

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'some-consumer',
  logLevel: logLevel.NOTHING,
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'some-consumer' });

const run = async () => {
  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({ topic: 'sum-requests', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const headers = parseHeaders(message.headers);
      const value = message.value?.toString();
      console.log('received message', value, headers);

      // headers validation
      const { correlationId, replyTo } = headers;
      if (!correlationId) {
        throw new Error('missing correlationId in headers');
      }

      if (!replyTo) {
        throw new Error('missing replyTo in headers');
      }

      await producer.send({
        topic: replyTo,
        messages: [
          {
            headers: { correlationId },
            value: value?.toUpperCase() ?? null,
          },
        ],
      });

      console.log('replied successfully to message', correlationId);
    },
  });

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
