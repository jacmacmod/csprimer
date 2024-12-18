import { CsvParseStream } from "@std/csv/parse-stream";
import { row, rowItem } from "./type.ts";
import { HeapFile } from "./page.ts";

type selectionFunction = (r: row) => boolean;

export type Nodeq =
  | MemoryScan
  | CSVFileScan
  | DataFileScan
  | LimitNode
  | ProjectionNode
  | SelectionNode;

export class DataFileScan {
  path: string;
  pageSize: number;
  heapFile: HeapFile | undefined;

  constructor(path: string, pageSize: number) {
    this.path = path;
    this.pageSize = pageSize;
  }

  async next() {
    if (!this.heapFile) await this.load();
    if (!this.heapFile) Deno.exit(1);
    const result: row | null = this.heapFile.read();

    if (result) return result;
    const page = await this.heapFile.nextPage();

    if (!page) return null;
    return this.heapFile.read();
  }

  async load() {
    this.heapFile = new HeapFile(this.path, this.pageSize);
    await this.heapFile.load();
  }

  stop() {
    if (this.heapFile) this.heapFile.stop();
    console.log("File closed.");
    return null;
  }
}

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

export class MemoryScan {
  idx: number;
  table: rowItem[][];
  schema: string[][];
  sorted: boolean = false;

  constructor(table: rowItem[][], schema: string[][]) {
    this.table = table;
    this.schema = schema;
    this.idx = 0;
  }

  next() {
    if (this.idx >= this.table.length) {
      return null;
    }

    const row = this.zip(this.table[this.idx]);
    this.idx += 1;
    return row;
  }

  zip(values: rowItem[]): row {
    const schema = this.schema;
    return values.reduce((obj, element, i) => {
      obj[schema[i][0]] = element;
      return obj;
    }, {} as row);
  }
}

export class ProjectionNode {
  child: Nodeq | undefined;
  columns: string[];

  constructor(colunms: string[]) {
    this.columns = colunms;
  }

  async next(): Promise<row | null> {
    const row = await this.child?.next();

    if (row === null || row === undefined) return null;
    if (Object.keys(row).length === 0) return {} as row;
    return Object.fromEntries(
      Object.entries(row).filter(([k]) => this.columns.includes(k))
    );
  }
}

export class SelectionNode {
  child: Nodeq | undefined;
  fn: selectionFunction;

  constructor(fn: selectionFunction) {
    this.fn = fn;
  }

  async next(): Promise<row | null> {
    const row = await this.child?.next();
    if (row === null || row === undefined) return null;
    if (Object.keys(row).length === 0) return {} as row;
    if (this.fn(row)) return row;

    return {} as row;
  }
}

export class LimitNode {
  child: Nodeq | undefined;
  n: number;
  count: number;

  constructor(n: number) {
    this.n = n;
    this.count = 0;
  }

  async next(): Promise<row | null> {
    if (this.count === this.n) {
      if (
        this.child instanceof CSVFileScan ||
        this.child instanceof DataFileScan
      ) {
        this.child.stop();
      }
      return null;
    }

    const row = await this.child?.next();
    this.count++;

    if (row === null || row === undefined) return null;
    if (Object.keys(row).length === 0) return {} as row;

    return row;
  }
}
