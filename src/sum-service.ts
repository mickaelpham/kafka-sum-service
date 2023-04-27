import { CompressionTypes, Kafka, logLevel } from 'kafkajs';

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'sum-service-client',
  logLevel: logLevel.ERROR,
});

const SUM_REQUEST_TOPIC = 'sum.request';

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'sum-service-consumer' });

const run = async () => {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: SUM_REQUEST_TOPIC, fromBeginning: true });

  console.log('running sum service');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const { headers } = message;
      if (!headers) {
        throw new Error('missing headers');
      }

      const { a, b } = JSON.parse(message.value?.toString() ?? '{}');
      const { replyTo: replyToBuffer, correlationId: correlationIdBuffer } =
        message.headers;

      if (replyToBuffer === undefined) {
        throw new Error('replyTo is undefined');
      }
      const replyTo = replyToBuffer.toString();

      if (correlationIdBuffer === undefined) {
        throw new Error('correlationId is undefined');
      }
      const correlationId = correlationIdBuffer.toString();

      console.log('processing message', replyTo, correlationId, a, b);

      if (typeof replyTo !== 'string' || typeof correlationId !== 'string') {
        throw new Error('invalid replyTo or correlationId');
      }

      // actually do the work
      const sum = a + b;

      // reply
      producer.send({
        topic: replyTo,
        compression: CompressionTypes.None,
        messages: [
          {
            value: JSON.stringify({ sum }),
            headers: { correlationId },
          },
        ],
      });
    },
  });
};

run().catch(console.error);
