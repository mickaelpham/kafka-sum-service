import { CompressionTypes, Kafka, logLevel } from 'kafkajs';
import { ulid } from 'ulid';

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'calculator-client',
  logLevel: logLevel.ERROR,
});

const producer = kafka.producer();

const SUM_REQUEST_TOPIC = 'sum.request';
const SUM_REPLY_TOPIC = 'sum.reply';

const sum = async (a: number, b: number): Promise<number> => {
  const correlationId = ulid();

  const consumer = kafka.consumer({
    groupId: `calculator-consumer-${correlationId}`,
  });

  await consumer.connect();
  await consumer.subscribe({ topic: SUM_REPLY_TOPIC, fromBeginning: true });

  return new Promise<number>((resolve) => {
    console.log('awaiting reply', correlationId);
    consumer.run({
      eachMessage: async ({ message }) => {
        const replyCorrelationId = message.headers?.correlationId?.toString();
        console.log('got a response', replyCorrelationId);

        if (replyCorrelationId === correlationId) {
          const { sum } = JSON.parse(message.value?.toString() ?? '{}');
          resolve(sum);
        }
      },
    });

    console.log('sending message to', SUM_REQUEST_TOPIC);
    producer.send({
      topic: SUM_REQUEST_TOPIC,
      compression: CompressionTypes.None,
      messages: [
        {
          value: JSON.stringify({ a, b }),
          headers: { replyTo: SUM_REPLY_TOPIC, correlationId },
        },
      ],
    });
  });
};

const run = async () => {
  console.log('about to connect producer');
  await producer.connect();

  let n1 = 0;
  let n2 = 1;
  let nextTerm: number;

  for (let i = 0; i < 10; i++) {
    console.log(n1);
    nextTerm = await sum(n1, n2);
    n1 = n2;
    n2 = nextTerm;

    // await sleep(1_000);
  }

  await producer.disconnect();
};

run().catch(console.error);
