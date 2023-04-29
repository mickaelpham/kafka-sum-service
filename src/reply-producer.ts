import { Kafka, logLevel } from 'kafkajs';
import replyContainer from './reply-container';
import { parseHeaders } from './utils';

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'some-consumer',
  logLevel: logLevel.NOTHING,
});

const run = async (): Promise<void> => {
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

  for (const response of responses) {
    if (response.status === 'fulfilled') {
      const message = response.value;
      const headers = parseHeaders(message.headers);
      const value = message.value?.toString();
      console.log('received back message', headers, value);
    }
  }

  container
    .stop()
    .then(() => {
      console.log('reply container stopped');
    })
    .catch(console.error);
};

run().catch(console.error);
