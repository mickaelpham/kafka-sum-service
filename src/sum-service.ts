import { Kafka, logLevel } from 'kafkajs';
import { parseHeaders } from './utils';

const TIMEOUT_DURATION = 30 * 1_000; // milliseconds

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'sum-service-client',
  logLevel: logLevel.ERROR,
});

const SUM_REQUEST_TOPIC = 'sum-requests';

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'sum-service-consumer' });

let messagesHandledCount = 0;

const run = async () => {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: SUM_REQUEST_TOPIC, fromBeginning: false });

  console.log('running sum service');

  await consumer.run({
    eachMessage: async ({ message: { headers, value } }) => {
      messagesHandledCount += 1;

      // headers validation
      const { correlationId, replyTo } = parseHeaders(headers);
      if (!correlationId) {
        throw new Error('missing correlationId in headers');
      }

      if (!replyTo) {
        throw new Error('missing replyTo in headers');
      }

      // body validation
      const { a, b } = JSON.parse(value ? value.toString() : '{}');
      if (typeof a !== 'number' || Number.isNaN(a)) {
        throw new Error('a is not a number');
      }

      if (typeof b !== 'number' || Number.isNaN(b)) {
        throw new Error('b is not a number');
      }

      console.log(
        `processing message [a=${a}, b=${b}, replyTo=${replyTo}, correlationId=${correlationId}]`,
      );

      // actually do the work
      const sum = a + b;

      // reply
      producer.send({
        topic: replyTo,
        messages: [
          {
            value: JSON.stringify({ sum }),
            headers: { correlationId },
          },
        ],
      });
    },
  });

  // set a timeout to disconnect both producer and consumer
  setTimeout(() => {
    console.log(`sum service handled ${messagesHandledCount} messages`);
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
