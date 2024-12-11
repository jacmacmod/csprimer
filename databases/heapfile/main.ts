import { CsvParseStream } from "@std/csv/parse-stream";
import { parseArgs } from "@std/cli/parse-args";

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

type encodedRow = Uint8Array;

// write binary to file movies.data and read it back out
// add support for slotted pages
// incorporate as fileScan
// maybe support null maps

// page index (4 byte)
// upper index
// lower index
// pointers list

// row from bottom pointer_id row -> row info
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

async function main() {
  // const flags = parseArgs(Deno.args, {
  //   string: ["csv"],
  //   default: { csv: "../ml-20m/movies.csv" },
  // });

  // const gen = run(Q([new CSVFileScan(flags.csv)]));
  // // open file for writing
  // const file = await Deno.create("movies.data");
  // const writer = file.writable.getWriter();

  
  // for await (const row of gen) {
  //   const encodedRow = encode(row);
  //   writer.write(encodedRow);
  // }
  // writer.close();

  const dataFile = await Deno.open("movies.data", { read: true });
  const lenBuffer = new Uint8Array(2);
  let x = 0
  while (true) {
    const bytesRead = await dataFile.read(lenBuffer);
    if (bytesRead === null || bytesRead === 0) break;
    console.log(lenBuffer)
    const newBuf = new Uint8Array(lenBuffer[0] + (lenBuffer[1] << 8))
    await dataFile.read(newBuf);
    const row = decode(newBuf);
    console.log(row)
    console.log(newBuf)
    x++ 
  }
  
  dataFile.close()
  // let bytesRead = dataFile.read(buffer);
  
  // const inputReader = dataFile.readable.getReader();
  // while (true) {
  //   const result = await inputReader.read();
  //   if (result.done) {
  //     break;
  //   }
  //   console.log(result.value, result.value?.length, decode(result.value))
  // }
}

main();
