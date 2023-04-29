import { type Kafka, type Message } from 'kafkajs';
import { ulid } from 'ulid';
import { parseHeaders } from './utils';

interface ReplyContainer {
  start: () => Promise<void>;
  stop: () => Promise<void>;
  sendAndReceive: (args: {
    topic: string;
    message: Message;
  }) => Promise<Message>;
}

export default ({
  kafka,
  groupId,
  replyTopic,
}: {
  kafka: Kafka;
  groupId: string;
  replyTopic: string;
}): ReplyContainer => {
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId });
  const replies = new Map<string, (value: Message) => void>();

  return {
    start: async () => {
      await Promise.all([producer.connect(), consumer.connect()]);
      await consumer.subscribe({ topic: replyTopic, fromBeginning: false });
      await consumer.run({
        eachMessage: async ({ message }) => {
          const { correlationId } = parseHeaders(message.headers);
          if (correlationId === undefined) {
            console.error('received message without correlationId');
            return;
          }

          const resolve = replies.get(correlationId);
          if (resolve === undefined) {
            console.log('message is not for this reply container');
            return;
          }

          resolve(message);
        },
      });
    },

    stop: async () => {
      await Promise.all([producer.disconnect(), consumer.disconnect()]);
    },

    sendAndReceive: async ({ topic, message }) =>
      await new Promise((resolve, reject) => {
        const correlationId = ulid();

        // add correlationId and replyTo headers
        const messageWithHeaders = {
          ...message,
          headers: {
            ...message.headers,
            correlationId,
            replyTo: replyTopic,
          },
        };

        // store the callback in the container replies
        replies.set(correlationId, resolve);

        // send and forget about the message
        producer.send({ topic, messages: [messageWithHeaders] }).catch(reject);
      }),
  };
};
