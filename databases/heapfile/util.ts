import { CSVFileScan, DataFileScan, MemoryScan, Nodeq } from "./nodes.ts";

import { row, encodedRow } from "./type.ts";

// export function Q(nodes: Array<Nodeq>): Nodeq {
//   const ns = nodes[Symbol.iterator]();
//   const root: Nodeq = ns.next().value!;
//   return root;
// }

export function Q(nodes: Array<Nodeq>): Nodeq {
  const ns = nodes[Symbol.iterator]();
  const root: Nodeq = ns.next().value!;
  let parent: Nodeq = root;

  for (const n of ns) {
    if (
      !(parent instanceof CSVFileScan) &&
      !(parent instanceof MemoryScan) &&
      !(parent instanceof DataFileScan)
    ) {
      parent.child = n;
      parent = n;
    }
  }
  return root;
}

export async function* run(q: Nodeq) {
  while (true) {
    const row = await q.next();
    if (row && typeof row === "object" && Object.keys(row).length === 0)
      continue;
    if (!row) break;

    yield await Promise.resolve(row);
  }
}

// encoders decoders
export function encode(row: row): encodedRow {
  const movieId = encodeNumber(Number(row.movieId) as number);
  const title = encodeString(row.title as string);
  const genres = encodeString(row.genres as string);
  const rowLength = movieId.length + title.length + genres.length;
  const size = new Uint8Array(2);
  size[0] = rowLength & 0xff;
  size[1] = (rowLength >> 8) & 0xff;
  return new Uint8Array([...size, ...movieId, ...title, ...genres]);
}

export function decode(row: encodedRow): row {
  let idx = 0;
  const movieID = decodeNumber(row.slice(0, 4));
  idx = 4;
  const title = decodeString(row.slice(idx, idx + 1 + row[idx]));
  idx = idx + row[idx] + 2;
  const genres = decodeString(row.slice(idx, idx + 1 + row[idx]));
  return {
    movieId: String(movieID),
    title: title,
    genres: genres,
  } as row;
}

export function encodeString(str: string): Uint8Array {
  if (str.length > 255) {
    console.error("string too long");
  }

  const textEncoder = new TextEncoder();
  const encoded = textEncoder.encode(str);
  return new Uint8Array([encoded.length, ...encoded, 0]);
}

export function decodeString(arr: Uint8Array): string {
  const decoder = new TextDecoder();
  return decoder.decode(arr.slice(1, arr[0] + 1));
}

export function encodeNumber(n: number): Uint8Array {
  const arr = new Uint8Array(4);
  arr[0] = n & 0xff;
  arr[1] = (n >> 8) & 0xff;
  arr[2] = (n >> 16) & 0xff;
  return arr;
}

export function decodeNumber(arr: Uint8Array): number {
  return arr[0] + (arr[1] << 8) + (arr[2] << 16);
}

export function encode2byteInt(n: number): number[] {
  const arr = new Array(2);
  arr[0] = n & 0xff;
  arr[1] = (n >> 8) & 0xff;
  return arr;
}

export function decodeNumber2bytesAsNumber(arr: Uint8Array): number {
  return arr[0] + (arr[1] << 8);
}
