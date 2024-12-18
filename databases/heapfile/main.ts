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

export function decodeNumber2bytesAsNumber(arr: Uint8Array): number {
  return arr[0] + (arr[1] << 8);
}

function encode2byteInt(n: number): number[] {
  const arr = new Array(2);
  arr[0] = n & 0xff;
  arr[1] = (n >> 8) & 0xff;
  return arr;
}

class Page {
  id: number;
  pageSize: number = 8096;

  upperOffset: number = 6;
  lowerOffset: number;

  currentRowId: number = 0;

  rowOffsets: number[] = [];
  rows: number[][] = [];

  constructor(pageId: number, pageSize: number) {
    // pageid (2) lower(2) upper(2) (rowItem(2)...n items))
    this.id = pageId;
    this.pageSize = pageSize;

    this.lowerOffset = this.pageSize;
  }

  addRow(row: number[]) {
    this.rows?.push(row);
    this.upperOffset += 2;
    this.lowerOffset -= row.length;
    this.rowOffsets.push(this.lowerOffset);
  }

  encodeRow(row: row): number[] {
    const movieId = this.encodeNumber(Number(row.movieId) as number);
    const title = encodeString(row.title as string);
    const genres = encodeString(row.genres as string);

    return [...movieId, ...title, ...genres];
  }

  encodeString(str: string): number[] {
    if (str.length > 255) {
      console.error("string too long");
    }
    const textEncoder = new TextEncoder();
    const encoded = textEncoder.encode(str);
    return Array.from(new Uint8Array([encoded.length, ...encoded, 0]));
  }

  encodeNumber(n: number): number[] {
    const arr = new Array(4).fill(0);
    arr[0] = n & 0xff;
    arr[1] = (n >> 8) & 0xff;
    arr[2] = (n >> 16) & 0xff;
    return arr;
  }

  createBuffer(): Uint8Array {
    // pageid (2) lower(2) upper(2) (rowItem(2)...n items))
    const rowOffsets: number[] = [];
    for (const v of this.rowOffsets) {
      const arr = encode2byteInt(v);
      rowOffsets.push(arr[0]);
      rowOffsets.push(arr[1]);
    }

    let arr = [
      ...encode2byteInt(this.id),
      ...encode2byteInt(this.lowerOffset),
      ...encode2byteInt(this.upperOffset),
      ...rowOffsets,
      ...new Array(this.lowerOffset - this.upperOffset).fill(0),
    ];

    if (this.lowerOffset - this.upperOffset < 2) {
      console.log(this.id, this.lowerOffset - this.upperOffset, "death");
      Deno.exit(1);
    }

    for (let i = this.rows!.length - 1; i >= 0; i--) {
      arr = [...arr, ...this!.rows[i]];
    }

    if (arr.length !== this.pageSize) {
      console.log(
        arr,
        new Array(this.lowerOffset - this.upperOffset).fill(0).length
      );
      Deno.exit(1);
    }
    return new Uint8Array(arr);
  }
}

async function writeHeapFile(pageSize: number) {
  const flags = parseArgs(Deno.args, {
    string: ["csv"],
    default: { csv: "../ml-20m/movies.csv" },
  });

  const gen = run(Q([new CSVFileScan(flags.csv)]));

  const file = await Deno.create("movies.data");
  const writer = file.writable.getWriter();

  let pageNumber: number = 1;
  let page = new Page(pageNumber, pageSize);
  let totalMovies = 0;

  for await (const row of gen) {
    const encodedRow = page.encodeRow(row);
    totalMovies++;

    if (page.lowerOffset - page.upperOffset < encodedRow.length + 6) {
      writer.write(page.createBuffer());
      pageNumber++;
      page = new Page(pageNumber, pageSize);
    }
    page.addRow(encodedRow);
  }

  console.log("total movies", totalMovies, "page number", pageNumber);

  writer.write(page.createBuffer());
  writer.close();
}

async function readHeapFile(pageSize: number) {
  const file = await Deno.open("movies.data", { read: true });

  const buf = new Uint8Array(pageSize);
  let pageCount = 1;
  let totalMovies = 0;

  try {
    let bytesRead: number | null;

    while ((bytesRead = await file.read(buf)) !== null) {
      if (bytesRead > 0) {
        let x = 6; // Start offset for data within the page
        let max = buf.length;

        while (true) {
          const curr = decodeNumber2bytesAsNumber(buf.slice(x, x + 2));

          if (curr === 0) {
            break;
          }

          const row = decode(buf.slice(curr, max));
          console.log(row);

          if (!row || row.movieId === "NaN") {
            console.error("Invalid row data, terminating...");
            return;
          } else {
            totalMovies++;
          }

          x += 2; // Move to the next offset
          max = curr;
        }
        pageCount++;
      }
    }
  } finally {
    file.close();
  }
  console.log(pageCount);
  console.log(totalMovies);
}

async function main() {
  const pageSize = 1024;

  await writeHeapFile(pageSize);
  await readHeapFile(pageSize);
}

main();

// let bytesRead = dataFile.read(buffer);

// const inputReader = dataFile.readable.getRea der();
// while (true) {
//   const result = await inputReader.read();
//   if (result.done) {
//     break;
//   }
//   console.log(result.value, result.value?.length, decode(result.value))
// }
