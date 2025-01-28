import { CsvParseStream } from "jsr:@std/csv";
import { column, row } from "./type.ts";
import { defaultPageSize, defaultTableLocation, HeapFile } from "./page.ts";
import { columnDefinition, getSchema } from "./util.ts";

export type selectionFunction = (r: row) => boolean;
export type projectionFunction = (r: row) => row;
export type Nodeq =
  | MergeJoin
  | NestedLoopJoin
  | HashJoin
  | Sort
  | MemoryScan
  | CSVFileScan
  | DataFileScan
  | Limit
  | Projection
  | Count
  | Selection;

export class DataFileScan {
  table: string;
  child: Nodeq | null = null;
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
    if (this.heapFile.done) return null;

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
    if (this.heapFile && !this.heapFile.done) this.heapFile.stop();

    return null;
  }

  async reset() {
    if (this.heapFile?.file && !this.heapFile.done) {
      await this.heapFile.reset();
    } else {
      await this.load();
    }
  }
}

export class CSVFileScan {
  child: Nodeq | null = null;
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

    if (result?.done) return null;

    return result?.value;
  }

  async initializeCsvReader() {
    this.csv = await Deno.open(this.fileName, { read: true });

    this.reader = this.csv.readable
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(new CsvParseStream())
      .getReader();

    // use first column for schema
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

    return null;
  }
}

export class NestedLoopJoin {
  left: Nodeq;
  right: Nodeq;
  child: Nodeq | null = null;

  leftRow: row | null | undefined;

  initialized: boolean = false;

  constructor(left: Nodeq, right: Nodeq) {
    this.left = left;
    this.right = right;

    if (!this.left) Deno.exit(1);
    if (!this.right) Deno.exit(1);
  }

  async next(): Promise<row | null> {
    if (!this.initialized) {
      if ((this.leftRow = await this.left.next()) === null) {
        this.stop();
        return null;
      }
      this.initialized = true;
    }

    let rightRow;
    if ((rightRow = await this.right.next()) === null) {
      if (
        this.right instanceof DataFileScan ||
        this.right instanceof MemoryScan
      ) {
        await this.right.reset();
      }
      rightRow = await this.right.next();
    } else {
      if (this.leftRow && rightRow) return [...this.leftRow, ...rightRow];
    }

    if ((this.leftRow = await this.left.next()) === null) {
      this.stop();
      return null;
    }
    if (this.leftRow && rightRow) return [...this.leftRow, ...rightRow];

    this.stop();
    return null;
  }

  stop() {
    if (this.left instanceof CSVFileScan || this.left instanceof DataFileScan) {
      this.left.stop();
    }

    if (
      this.right instanceof CSVFileScan ||
      this.right instanceof DataFileScan
    ) {
      this.right.stop();
    }
  }
}

// InnerJoin
export class HashJoin {
  left: Nodeq | undefined;
  right: Nodeq | undefined;
  child: Nodeq | null = null;

  hashKeyLeft: (r: row) => column;
  hashKeyRight: (r: row) => column;

  hashTable: { [key: string]: row[] } | undefined = undefined;
  bucket: row[] = [];
  bucketIdx = 0;

  rightRow: string[] | row | null | undefined;

  constructor(
    left: Nodeq,
    right: Nodeq,
    hashKeyLeft: (r: row) => column,
    hashKeyRight: (r: row) => column
  ) {
    this.left = left;
    this.right = right;
    this.hashKeyLeft = hashKeyLeft;
    this.hashKeyRight = hashKeyRight;
  }

  async next(): Promise<row | null> {
    if (!this.hashTable) {
      this.hashTable = {};
      while (true) {
        const row = await this.left?.next();
        if (!row) break;

        const key = String(this.hashKeyLeft(row)) as string;
        if (!this.hashTable[key]) this.hashTable[key] = [];
        this.hashTable[key].push(row);
      }
    }

    while (this.bucketIdx >= this.bucket.length) {
      this.rightRow = await this.right?.next();
      if (!this.rightRow) return null;

      const hashKey = String(this.hashKeyRight(this.rightRow)) as string;
      this.bucket = this.hashTable && this.hashTable[hashKey];
      this.bucketIdx = 0;
    }

    const leftRow = this.bucket[this.bucketIdx];
    this.bucketIdx++;

    if (leftRow && this.rightRow) {
      return [...leftRow, ...this.rightRow];
    }
    return null;
  }

  stop() {
    if (this.left instanceof CSVFileScan || this.left instanceof DataFileScan) {
      this.left.stop();
    }

    if (
      this.right instanceof CSVFileScan ||
      this.right instanceof DataFileScan
    ) {
      this.right.stop();
    }
  }

  reset() {
    this.bucket = [];
    this.bucketIdx = 0;
    this.hashTable = undefined;
    this.rightRow = null;
  }
}

export class MergeJoin {
  left: Nodeq | null = null;
  right: Nodeq | null = null;
  child: Nodeq | null = null;

  sortKeyLeft: (r: row) => column;
  sortKeyRight: (r: row) => column;

  leftRow: row | null = null;
  rightRow: row | null = null;

  buf: row[] = [];
  bidx: number = 0;
  mode: string = "fetch";

  constructor(
    left: Nodeq,
    right: Nodeq,
    sortKeyLeft: (r: row) => column,
    sortKeyRight: (r: row) => column
  ) {
    this.left = left;
    this.right = right;
    this.sortKeyLeft = sortKeyLeft;
    this.sortKeyRight = sortKeyRight;
  }

  async next(): Promise<row | null> {
    if (!this.leftRow) {
      this.leftRow = (await this.left?.next()) as row | null;
      if (!this.leftRow) return null;
    }

    if (this.mode === "fetch") {
      this.rightRow = (await this.right?.next()) as row | null;
    }

    if (this.mode === "branch" && this.bidx < this.buf.length) {
      console.log("branch");
      const right = this.buf[this.bidx];
      this.bidx++;
      return [...this.leftRow, ...right];
    }

    if (
      this.leftRow &&
      this.rightRow &&
      this.sortKeyLeft(this.leftRow) === this.sortKeyRight(this.rightRow)
    ) {
      this.buf.push(this.rightRow);
      return [...this.leftRow, ...this.rightRow];
    }

    const leftRow = (await this.left?.next()) as row | null;
    if (leftRow === null) return null;

    this.bidx = 0;
    if (this.sortKeyLeft(leftRow) === this.sortKeyLeft(this.leftRow)) {
      this.mode = "branch";
    } else {
      this.buf = [];
      this.mode === "fetch";
      this.leftRow = leftRow;
      if (
        this.leftRow &&
        this.rightRow &&
        this.sortKeyLeft(this.leftRow) === this.sortKeyRight(this.rightRow)
      )
        return [...this.leftRow, ...this.rightRow];
    }

    this.leftRow = leftRow;
    return await this.next();
  }

  stop() {
    if (this.left instanceof CSVFileScan || this.left instanceof DataFileScan) {
      this.left.stop();
    }

    if (
      this.right instanceof CSVFileScan ||
      this.right instanceof DataFileScan
    ) {
      this.right.stop();
    }
  }
}

export class MemoryScan {
  table: row[];
  schema: columnDefinition[] = [];
  child: Nodeq | null = null;

  idx: number;
  sorted: boolean = false;

  constructor(table: row[], schema: columnDefinition[]) {
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

  stop() {}
}

export class Projection {
  child: Nodeq | null = null;
  fn: projectionFunction | undefined;

  constructor(fn: projectionFunction) {
    this.fn = fn;
  }

  async next(): Promise<row | null> {
    if (!this.fn) Deno.exit(1);
    const row = await this.child?.next();

    if (!row) return null;

    return this.fn(row);
  }

  reset() {
    if (
      this.child instanceof DataFileScan ||
      this.child instanceof MemoryScan ||
      this.child instanceof Sort ||
      this.child instanceof Selection
    ) {
      this.child?.reset();
    }
  }
}

export class Count {
  child: Nodeq | null = null;
  done: boolean = false;

  async next(): Promise<row | null> {
    if (this.child === null || this.done) return null;

    let count = 0;
    while ((await this.child?.next()) !== null) {
      count++;
    }
    this.done = true;

    return [count];
  }
}

export class Selection {
  child: Nodeq | null = null;
  predicate: selectionFunction;

  constructor(predicate: selectionFunction) {
    this.predicate = predicate;
  }

  async next(): Promise<row | null> {
    while (true) {
      const row = await this.child?.next();
      if (row === null || this.predicate(row as row)) {
        return row as row;
      }
    }
  }

  reset() {
    if (
      this.child instanceof DataFileScan ||
      this.child instanceof MemoryScan ||
      this.child instanceof Sort ||
      this.child instanceof Selection
    )
      this.child?.reset();
  }

  stop() {
    if (this.child && this.child instanceof NestedLoopJoin) {
      this.child?.stop();
    }
  }
}

export class Limit {
  child: Nodeq | null = null;
  n: number;
  count: number;

  constructor(n: number) {
    this.n = n;
    this.count = 0;
  }

  async next(): Promise<row | null> {
    if (this.count === this.n) {
      if (
        this.child &&
        (this.child instanceof CSVFileScan ||
          this.child instanceof DataFileScan ||
          this.child instanceof NestedLoopJoin ||
          this.child instanceof HashJoin ||
          this.child instanceof Selection)
      ) {
        this.child?.stop();
      }

      return null;
    }

    const row = await this.child?.next();
    this.count++;

    if (row) return row;
    return null;
  }
}

export class Sort {
  sortKey: (r: row) => column;
  sign: number = 1;
  child: Nodeq | undefined;
  sortedRows: row[] = [];
  idx: number = 0;

  constructor(sortKey: (r: row) => column, desc: boolean = false) {
    console.log("sort constructor");
    this.sortKey = sortKey;
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
      const fieldA = this.sortKey(a);
      const fieldB = this.sortKey(b);
      if (fieldA < fieldB) return this.sign * 1;
      if (fieldA > fieldB) return this.sign * -1;
      return 0;
    });
  }

  reset() {
    if (
      this.child instanceof DataFileScan ||
      this.child instanceof MemoryScan ||
      this.child instanceof Sort ||
      this.child instanceof Selection
    )
      this.child?.reset();
  }
}
