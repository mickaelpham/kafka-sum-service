import { Kafka, logLevel } from 'kafkajs';
import replyContainer from './reply-container';
import { ulid } from 'ulid';

const FIBONACCI_UP_TO_TERM = 100;

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'fibonacci',
  logLevel: logLevel.NOTHING,
});

const run = async (): Promise<void> => {
  const container = replyContainer({
    kafka,
    groupId: `fibonacci-consumer-${ulid()}`,
    replyTopic: 'sum-replies',
  });

  await container.start();

  let n1 = 0;
  let n2 = 1;

  for (let i = 0; i < FIBONACCI_UP_TO_TERM; i++) {
    console.log(n1);

    const response = await container.sendAndReceive({
      topic: 'sum-requests',
      message: { value: JSON.stringify({ a: n1, b: n2 }) },
    });

    const { sum } = JSON.parse(
      response.value !== null ? response.value.toString() : '{}',
    );
    if (typeof sum !== 'number' || Number.isNaN(sum)) {
      throw new Error('invalid sum in response');
    }

    n1 = n2;
    n2 = sum;
  }

  container
    .stop()
    .then(() => {
      console.log('reply container stopped');
    })
    .catch(console.error);
};

run().catch(console.error);
