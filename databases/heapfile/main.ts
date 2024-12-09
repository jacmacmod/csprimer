import { CsvParseStream } from "@std/csv/parse-stream";
import { parseArgs } from "jsr:@std/cli/parse-args";

type row = {
  [key: string]: rowItem;
};
type rowItem = string | number | boolean;
type Nodeq = CSVFileScan;

export class CSVFileScan {
  path: string;
  csv: Deno.FsFile | undefined;
  schema: string[] | undefined;
  reader: ReadableStreamDefaultReader<string[]> | undefined;

  constructor(path: string) {
    this.path = path;
  }

  async next() {
    if (this.csv === undefined) await this.initializeCsvReader();

    const result = await this.reader?.read();

    if (result?.done) {
      console.log("File closed.");
      return null;
    }
    return result?.value && this.zip(Object.values(result.value));
  }

  async initializeCsvReader() {
    this.csv = await Deno.open(this.path, { read: true });
    this.reader = this.csv.readable
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(new CsvParseStream())
      .getReader();

    // use first rowItem for schema
    const result = await this.reader?.read();
    // csv library closes file automatically when reaching the end
    if (result && !result.done) this.schema = result.value;
  }

  stop() {
    if (this.csv) this.csv.close();

    console.log("File closed.");
    return null;
  }

  zip(values: string[]): row {
    const schema = this.schema!;
    return values.reduce((obj, element, i) => {
      obj[schema[i]] = element;
      return obj;
    }, {} as row);
  }
}

export function Q(nodes: Array<Nodeq>): Nodeq {
  const ns = nodes[Symbol.iterator]();
  const root: Nodeq = ns.next().value!;
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

// async function main() {
//   const flags = parseArgs(Deno.args, {
//     string: ["csv"],
//     default: { csv: "../ml-20m/movies.csv" },
//   });

//   const gen = run(Q([new CSVFileScan(flags.csv)]));

//   for await (const value of gen) {
//     console.log(value)
//   }
// }
// main()

type encodedRow = Uint8Array;

// supported types?

//
export function encode(row: row): encodedRow {
  // TODO row id handling
  // 2 null bytes of padding
  // row
  // id byte id starting from 0 (1 byte for now)
  //
  // int is 3 bytes with 1 bytes of padding anything bigger is not allowed for now
  // string has length and null terminator
  // for (const prop in row) {
  //   const propParsed = parseInt(prop);
  //   if (Number.isInteger(propParsed)) {
  //     console.log("what", propParsed);


  const movieId = encodeNumber(Number(row.movieId) as number);
  const title = encodeString(row.title as string);
  const genres = encodeString(row.genres as string);

  return new Uint8Array([...movieId, ...title, ...genres]);
}

export function decode(row: encodedRow): row {
  let idx = 0;
  const movieID = decodeNumber(row.slice(0, 4))
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
  return arr[0] + ((arr[1] << 8)) + ((arr[2] << 16));
}
