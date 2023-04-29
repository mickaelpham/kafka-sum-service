import { Kafka, logLevel } from 'kafkajs';
import replyContainer from './reply-container';

const TIMEOUT_DURATION = 10_000; // 10 seconds

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'some-consumer',
  logLevel: logLevel.NOTHING,
});

const run = async () => {
  const container = replyContainer({
    kafka,
    groupId: 'reply-consumer',
    replyTopic: 'sum-replies',
  });

  await container.start();

  const responses = await Promise.allSettled([
    container.sendAndReceive({
      topic: 'sum-requests',
      message: { value: 'foo' },
    }),
    container.sendAndReceive({
      topic: 'sum-requests',
      message: { value: 'bar' },
    }),
    container.sendAndReceive({
      topic: 'sum-requests',
      message: { value: 'baz' },
    }),
  ]);

  console.log(responses);

  container
    .stop()
    .then(() => console.log('reply container stopped'))
    .catch(console.error);
};

run().catch(console.error);
