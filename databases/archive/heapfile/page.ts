import { parseArgs } from "@std/cli/parse-args";

import {
  run,
  Q,
  decode,
  encode2byteInt,
  encodeString,
  decodeNumber2bytesAsNumber,
} from "./util.ts";

import { CSVFileScan } from "./nodes.ts";
import { row } from "./type.ts";

const defaultPageSize = 1024;

export class Page {
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
      Deno.exit(1);
    }

    for (let i = this.rows!.length - 1; i >= 0; i--) {
      arr = [...arr, ...this!.rows[i]];
    }

    if (arr.length !== this.pageSize) Deno.exit(1);

    return new Uint8Array(arr);
  }
}

export async function writeHeapFile(
  path: string = "../ml-20m/movies.csv",
  pageSize: number = defaultPageSize,
) {
  const flags = parseArgs(Deno.args, {
    string: ["csv"],
    default: { csv: path },
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

    // create at least 2 bytes of cushion
    if (page.lowerOffset - page.upperOffset < encodedRow.length + 6) {
      writer.write(page.createBuffer());
      pageNumber++;
      page = new Page(pageNumber, pageSize);
    }
    page.addRow(encodedRow);
  }

  writer.write(page.createBuffer());
  writer.close();
}

export async function readHeapFileALL(
  path: string = "movies.data",
  pageSize: number = defaultPageSize,
) {
  const file = await Deno.open(path, { read: true });
  const buf = new Uint8Array(pageSize);

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

          if (!row || row.movieId === "NaN") {
            console.error("Invalid row data, terminating...");
            return;
          }

          x += 2; // Move to the next offset
          max = curr;
        }
      }
    }
  } finally {
    file.close();
  }
}

export class HeapFile {
  path: string = "movies.data";
  pageSize: number = defaultPageSize;
  file: Deno.FsFile | undefined = undefined;
  buf: Uint8Array = new Uint8Array();
  done: boolean = false;
  lowerIdx: number = 6;
  prevOffset: number = defaultPageSize;
  offset: number = 0;
  currentPage: number = 1;

  constructor(path: string, pageSize: number) {
    this.path = path;
    this.pageSize = pageSize;
  }

  async load() {
    this.file = await Deno.open(this.path, { read: true });
    this.buf = new Uint8Array(this.pageSize);

    const numberOfBytesRead = await this.file.read(this.buf);
    if (numberOfBytesRead === null || numberOfBytesRead === 0) {
      this.done = true;
      this.file.close();
    }
    return this;
  }

  read() {
    const offsetBytes = this.buf.slice(this.lowerIdx, this.lowerIdx + 2);
    const offset = decodeNumber2bytesAsNumber(offsetBytes);
    const rowBytes = this.buf.slice(offset, this.prevOffset);
    const row = decode(rowBytes);

    if (offset === 0) return null;

    this.lowerIdx += 2; // Move to the next offset
    this.prevOffset = offset;

    return row;
  }

  async nextPage(): Promise<number | null> {
    if (!this.file) {
      console.log("cannot get next page: file does not exist");
      Deno.exit(1);
    }

    const numberOfBytesRead = await this.file.read(this.buf);
    if (numberOfBytesRead === null || numberOfBytesRead === 0) {
      this.done = true;
      this.file.close();
      return null;
    }

    this.lowerIdx = 6;
    this.prevOffset = this.pageSize;
    this.currentPage++;
    return this.currentPage;
  }

  stop() {
    if (this.file) this.file.close();

    this.done = true;
    this.file = undefined;
  }
}
