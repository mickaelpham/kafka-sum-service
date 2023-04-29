import { Kafka, Message } from 'kafkajs';
import { ulid } from 'ulid';
import { parseHeaders } from './utils';

export default ({
  kafka,
  groupId,
  replyTopic,
}: {
  kafka: Kafka;
  groupId: string;
  replyTopic: string;
}) => {
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId });
  const replies = new Map<string, (value: unknown) => void>();

  return {
    start: async () => {
      await Promise.all([producer.connect(), consumer.connect()]);
      await consumer.subscribe({ topic: replyTopic, fromBeginning: false });
      await consumer.run({
        eachMessage: async ({ message }) => {
          const { correlationId } = parseHeaders(message.headers);
          if (!correlationId) {
            console.error('received message without correlationId');
            return;
          }

          const resolve = replies.get(correlationId);
          if (!resolve) {
            console.log('message is not for this reply container');
            return;
          }

          const value = message.value?.toString();
          resolve(value);
        },
      });
    },

    stop: async () => {
      await Promise.all([producer.disconnect(), consumer.disconnect()]);
    },

    sendAndReceive: ({ topic, message }: { topic: string; message: Message }) =>
      new Promise((resolve, reject) => {
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

    // sendAndReceive: async (record: ProducerRecord) => {
    //   // add replyTo and correlationId headers
    //   const messagesWithHeaders = record.messages.map((message) => ({
    //     ...message,
    //     headers: {
    //       ...message.headers,
    //       correlationId: ulid(),
    //       replyTo: replyTopic,
    //     },
    //   }));

    //   // create a promise for each message to resolve
    //   messagesWithHeaders.forEach((message) => {
    //     replies.set(
    //       message.headers.correlationId,
    //       new Promise<void>((resolve) => {
    //         resolve();
    //       }),
    //     );
    //   });
    // },
  };
};
