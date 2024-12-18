
export type rowItem = string | number | boolean;

export type row = {
  [key: string]: rowItem;
};

export type encodedRow = Uint8Array;