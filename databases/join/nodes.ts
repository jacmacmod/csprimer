import { CsvParseStream } from "jsr:@std/csv";
import { rowItem } from "./type.ts";
import { defaultPageSize, defaultTableLocation, HeapFile } from "./page.ts";
import { columnDefinition, getSchema } from "./util.ts";

// export type selectionFunction = (r: row) => boolean;
// export type projectionFunction = (r: row) => row;
export type selectionFunction = (r: rowItem[]) => boolean;
export type projectionFunction = (r: rowItem[]) => rowItem[];
export type Nodeq =
  | NestedLoopJoin
  | Sort
  | MemoryScan
  | CSVFileScan
  | DataFileScan
  | Limit
  | Projection
  | Count
  | Selection;

// export class NestedLoopJoin {
//   table: string;
//   pageSize = defaultPageSize;
//   child: Nodeq | undefined;
//   scanNode: DataFileScan | undefined;
//   // heapFile: HeapFile | undefined;

//   constructor(table: string) {
//     this.table = table;
//   }

//   async next(): Promise<row | null> {
//     if (this.child === null) return {};

//     const subRow = await this.child?.next();
//     if (subRow === null || subRow === undefined) return null;
//     if (Object.keys(subRow).length === 0) return {} as row;

//     if (!this.scanNode) await this.load();
//     if (!this.scanNode) Deno.exit(1);
//     const row: row | null = await this.scanNode.next();

//     if (row) {
//       return { ...row, ...subRow };
//     } else {
//       return null;
//     }
//   }

//   async load() {
//     this.scanNode = new DataFileScan(this.table, this.pageSize);
//     if (!this.scanNode) Deno.exit(1);
//     await this.scanNode.load();
//   }

//   stop() {
//     if (this.scanNode) this.scanNode.stop();
//     console.log("File closed.");

//     return null;
//   }
// }
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

    const result = this.heapFile.read();

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

    return result?.value;
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
}

export class NestedLoopJoin {
  left: MemoryScan;
  right: MemoryScan;
  leftRow: rowItem[] | null = null;
  child: Nodeq | undefined;
  rightDone: boolean = false;

  constructor(left: MemoryScan, right: MemoryScan) {
    this.left = left;
    this.right = right;

    if (!this.left) Deno.exit(1);
    if (!this.right) Deno.exit(1);
  }

  next() {
    if (this.left.table.length === 0 || this.right.table.length === 0)
      return null;
    if (!this.leftRow) {
      this.leftRow = this.left.next();
      if (!this.leftRow) return null;
    }

    const rightRow = this.right.next();
    if (!rightRow) {
      this.rightDone = true;
      this.right.reset();
      this.leftRow = this.left.next();
    } else {
      return [...this.leftRow, ...rightRow];
    }
    return null;
  }
}
export class MemoryScan {
  idx: number;
  table: rowItem[][];
  schema: columnDefinition[] = [];
  sorted: boolean = false;

  constructor(table: rowItem[][], schema: columnDefinition[]) {
    this.table = table;
    this.schema = schema;
    this.idx = 0;
  }

  next() {
    if (this.idx >= this.table.length) return null;

    const row = this.table[this.idx];
    this.idx += 1;

    return row;
  }

  reset() {
    this.idx = 0;
  }
}

export class Projection {
  child: Nodeq | undefined;
  fn: projectionFunction | undefined;

  constructor(fn: projectionFunction) {
    this.fn = fn;
  }

  async next(): Promise<rowItem[] | null> {
    if (!this.fn) Deno.exit(1);
    const row = await this.child?.next();

    if (!row) return null;
    if (row.length === 0) return [];

    return this.fn(row);
  }
}

export class Count {
  child: Nodeq | undefined;
  done: boolean = false;

  async next(): Promise<rowItem[] | null> {
    if (this.child === null) return [];
    if (this.done) return null;

    let count = 0;
    while ((await this.child?.next()) !== null) {
      count++;
    }
    this.done = true;

    return [count];
  }
}

export class Selection {
  child: Nodeq | undefined;
  fn: selectionFunction;

  constructor(fn: selectionFunction) {
    this.fn = fn;
  }

  async next(): Promise<rowItem[] | null> {
    const row = await this.child?.next();

    if (!row) return null;
    if (row.length === 0) return [];
    if (this.fn(row)) return row;

    return [];
  }
}

export class Limit {
  child: Nodeq | undefined;
  n: number;
  count: number;

  constructor(n: number) {
    this.n = n;
    this.count = 0;
  }

  async next(): Promise<rowItem[] | null> {
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

    if (!row) return null;
    if (row.length === 0) return [];

    return row;
  }
}

export class Sort {
  colIdx: number = 0;
  sign: number = 1;
  child: Nodeq | undefined;
  sortedRows: rowItem[][] = [];
  idx: number = 0;

  constructor(colIdx: number, desc: boolean = false) {
    this.colIdx = colIdx;
    this.sign = desc ? 1 : -1;
  }

  async next() {
    if (this.idx === 0) await this.readAll();
    if (this.idx >= this.sortedRows.length) return null;

    const r = this.sortedRows[this.idx];
    this.idx++;

    return r;
  }

  private async readAll() {
    this.sortedRows = [];

    let r;
    while ((r = await this.child?.next()) !== null) {
      if (!r) break;
      this.sortedRows.push(r);
    }

    this.sort();
  }

  private sort() {
    this.sortedRows.sort((a, b) => {
      const fieldA = a[this.colIdx];
      const fieldB = b[this.colIdx];

      if (fieldA < fieldB) return this.sign * 1;
      if (fieldA > fieldB) return this.sign * -1;
      return 0;
    });
  }
}
