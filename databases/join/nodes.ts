import { CsvParseStream } from "jsr:@std/csv";
import * as path from "jsr:@std/path";
import { row, rowItem } from "./type.ts";
import { defaultPageSize, defaultTableLocation, HeapFile } from "./page.ts";
import { columnDefinition, getSchema } from "./util.ts";

export type selectionFunction = (r: row) => boolean;

export type Nodeq =
  | NestedLoopJoin
  | MemoryScan
  | CSVFileScan
  | DataFileScan
  | LimitNode
  | ProjectionNode
  | CountNode
  | SelectionNode;

export class NestedLoopJoin {
  table: string;
  pageSize = defaultPageSize;
  child: Nodeq | undefined;
  scanNode: DataFileScan | undefined;
  // heapFile: HeapFile | undefined;

  constructor(table: string) {
    this.table = table;
  }

  async next(): Promise<row | null> {
    if (this.child === null) return {};

    const subRow = await this.child?.next();
    if (subRow === null || subRow === undefined) return null;
    if (Object.keys(subRow).length === 0) return {} as row;

    if (!this.scanNode) await this.load();
    if (!this.scanNode) Deno.exit(1);
    const row: row | null = await this.scanNode.next();

    if (row) {
      return { ...row, ...subRow };
    } else {
      return null;
    }
  }

  async load() {
    this.scanNode = new DataFileScan(this.table, this.pageSize);
    if (!this.scanNode) Deno.exit(1);
    await this.scanNode.load();
  }

  stop() {
    if (this.scanNode) this.scanNode.stop();
    console.log("File closed.");

    return null;
  }
}
export class DataFileScan {
  table: string;
  pageSize: number;
  heapFile: HeapFile | undefined;
  dir: string = defaultTableLocation;

  constructor(
    path: string,
    pageSize: number = defaultPageSize,
    dir: string = defaultTableLocation
  ) {
    this.table = path;
    this.pageSize = pageSize;
    this.dir = dir;
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
    this.heapFile = new HeapFile(this.table, this.pageSize, this.dir);

    await this.heapFile.load();
  }

  stop() {
    if (this.heapFile) this.heapFile.stop();
    console.log("File closed.");

    return null;
  }
}

export class CSVFileScan {
  fileName: string;
  csv: Deno.FsFile | undefined;
  csvHeader: string[] | undefined;
  tableName: string;
  schema: columnDefinition[] | undefined;
  reader: ReadableStreamDefaultReader<string[]> | undefined;
  dir: string = defaultTableLocation;

  constructor(
    fileName: string,
    tableName: string,
    dir: string = defaultTableLocation
  ) {
    this.fileName = fileName;
    this.tableName = tableName;
    this.dir = dir;
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
    this.csv = await Deno.open(this.fileName, { read: true });

    this.reader = this.csv.readable
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(new CsvParseStream())
      .getReader();

    // use first rowItem for schema
    const result = await this.reader?.read();
    // csv library closes file automatically when reaching the end
    if (result && !result.done) this.csvHeader = result.value;

    this.schema = await getSchema(this.tableName, this.dir);

    // check that rows match schema
    if (!this.csvHeader?.every((h) => this.schema?.find((c) => c.name === h))) {
      console.log("CSV schema does not match");
      Deno.exit(1);
    }
  }

  stop() {
    if (this.csv) this.csv.close();

    console.log("File closed.");
    return null;
  }

  zip(values: string[]): row {
    const schema = this.csvHeader!;
    return values.reduce((row, element, i) => {
      row[schema[i]] = element;
      return row;
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
    if (this.idx >= this.table.length) return null;

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

export class CountNode {
  child: Nodeq | undefined;
  done: boolean = false;

  async next(): Promise<row | null> {
    if (this.child === null) return {};
    if (this.done) return null;

    let count = 0;
    while ((await this.child?.next()) !== null) {
      count++;
    }
    this.done = true;
    return { count: count };
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
