import { IHeaders } from 'kafkajs';

export const parseHeaders = (
  headers: IHeaders | undefined,
): Record<string, string | undefined> => {
  if (headers === undefined) {
    return {};
  }

  return Object.entries(headers).reduce(
    (acc, [key, value]) => ({
      ...acc,
      [key]: value?.toString(),
    }),
    {},
  );
};
