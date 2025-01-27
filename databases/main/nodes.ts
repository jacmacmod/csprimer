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
  leftRow: row | null | undefined;

  initialized: boolean = false;
  child: Nodeq | undefined;

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
  left: Nodeq | undefined;
  right: Nodeq | undefined;
  sortKeyLeft: (r: row) => column;
  sortKeyRight: (r: row) => column;

  leftTable: row[] = [];
  rightTable: row[] = [];

  leftIdx: number = 0;
  rightIdx: number = 0;

  rightDone: boolean = true;

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
    if (this.leftTable.length === 0) await this.init();

    const leftRow = this.leftTable[this.leftIdx];
    const rightRow = this.rightTable[this.rightIdx];

    if (!leftRow || !rightRow) return null;

    const leftKey = this.sortKeyLeft(leftRow);
    const rightKey = this.sortKeyRight(rightRow);

    if (leftKey === rightKey) {
      this.rightIdx++;
      return [...leftRow, ...rightRow];
    } else {
      this.leftIdx++;
      if (
        this.leftTable[this.leftIdx] &&
        leftKey === this.sortKeyLeft(this.leftTable[this.leftIdx])
      ) {
        // back track right table when the next left is the same as previous left
        let i = this.rightIdx - 1;
        while (i > 0) {
          if (this.sortKeyRight(this.rightTable[i]) == leftKey) {
            i--;
          } else {
            break;
          }
        }
        this.rightIdx = i;
      }
      return await this.next();
    }
  }

  private async init() {
    let row;
    while ((row = await this.left?.next())) {
      this.leftTable.push(row);
    }
    while ((row = await this.right?.next())) {
      this.rightTable.push(row);
    }

    this.sort(this.leftTable, this.sortKeyLeft);
    this.sort(this.rightTable, this.sortKeyRight);
  }

  private sort(table: row[], sortKey: (r: row) => column) {
    table.sort((a, b) => {
      const fieldA = sortKey(a);
      const fieldB = sortKey(b);

      if (fieldA < fieldB) return -1;
      if (fieldA > fieldB) return 1;
      return 0;
    });
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
  idx: number;
  table: row[];
  schema: columnDefinition[] = [];
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
  child: Nodeq | undefined;
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
  child: Nodeq | undefined;
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
  child: Nodeq | undefined;
  predicate: selectionFunction;

  constructor(predicate: selectionFunction, child?: Nodeq) {
    this.predicate = predicate;
    this.child = child;
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
  colIdx: number = 0;
  sign: number = 1;
  child: Nodeq | undefined;
  sortedRows: row[] = [];
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
