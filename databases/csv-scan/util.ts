import { CsvParseStream } from "@std/csv/parse-stream";
import { parseArgs } from "jsr:@std/cli/parse-args";

type row = {
  [key: string]: rowItem;
};
type rowItem = string | number | boolean;
// type projectionFunction = (r: row) => row;
type selectionFunction = (r: row) => boolean;
// type sortFunction = (r: row) => rowItem;
type Nodeq =
  | CSVFileScan
  | Limit
  | MemoryScan
  | Projection
  | Selection
  // | Sort;

class CSVFileScan {
  path: string;
  csv: Deno.FsFile | undefined;
  schema: string[] | undefined;
  reader: ReadableStreamDefaultReader<string[]> | undefined;

  constructor(path: string) {
    this.path = path;
  }

  async next() {
    console.log(this.csv)
    if (this.csv === undefined) await this.initializeCsvReader();

    const result = await this.reader?.read();
    if (result?.done) {
      this.stop();
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

class MemoryScan {
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

class Projection {
  child: Nodeq | undefined;
  columns: string[];

  constructor(colunms: string[]) {
    this.columns = colunms
  }

  async next(): Promise<row | null> {
    const row = await this.child?.next();
    
    console.log(row)
    
    if (row === null || row === undefined) return null;
    if (Object.keys(row).length === 0) return {} as row;
    console.log(Object.fromEntries(Object.entries(row).filter(([k]) => this.columns.includes(k))))
    return {"jack": "jack"}
    // return Object.fromEntries(Object.entries(row).filter(([k]) => this.columns.includes(k)));
  }
}

class Selection {
  child: Nodeq | undefined;
  fn: selectionFunction;

  constructor(fn: selectionFunction) {
    this.fn = fn;
  }

  async next(): Promise<row | null> {
    let row = await this.child?.next();
    row = await this.child?.next();
    row = await this.child?.next();
    row = await this.child?.next();
    console.log(row)
    if (row === null || row === undefined) return null;
    if (Object.keys(row).length === 0) return {} as row;
    if (this.fn(row)) return row;

    return {} as row;
  }
}

class Limit {
  child: Nodeq | undefined;
  n: number;
  count: number;

  constructor(n: number) {
    this.n = n;
    this.count = 0;
  }

  async next(): Promise<row | null> {
    if (this.count === this.n) {
      if (this.child instanceof CSVFileScan) this.child.stop();
      return null;
    }

    const row = await this.child?.next();
    this.count++;

    if (row === null || row === undefined) return null;
    if (Object.keys(row).length === 0) return {} as row;

    return row;
  }
}

function Q(nodes: Array<Nodeq>): Nodeq {
  const ns = nodes[Symbol.iterator]();
  const root: Nodeq = ns.next().value!;
  let parent: Nodeq = root;

  for (const n of ns) {
    if (parent instanceof Limit) {
      parent.child = n;
      parent = n;
    }
  }
  return root;
}

async function* run(q: Nodeq) {
  while (true) {
    const row = await q.next();
    if (row && typeof row === "object" && Object.keys(row).length === 0) continue;
    if (!row) break;

    yield await Promise.resolve(row);
  }
}

async function main() {
  const flags = parseArgs(Deno.args, {
    string: ["csv"],
    default: { csv: "/tmp/ml-20m/movies.csv" },
  });

  const gen = run(
    Q([
      // new Projection(["title"]),
      // new Limit(2),
      new Selection((row: row) => row["title"] === false),
      new CSVFileScan(flags.csv),
    ])
  );

  for await (const value of gen) {
    console.log(value);
  }
}

main();