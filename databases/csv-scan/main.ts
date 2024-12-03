import { CsvParseStream } from "@std/csv/parse-stream";

type row = string | number | boolean;
type projectionFunction = (arr: Array<row>) => Array<row>;
type selectionFunction = (arr: Array<row>) => boolean;
type sortFunction = (arr: Array<row>) => row;
type Nodeq =
  | CSVFileScan
  | LimitNode
  | MemoryScan
  | ProjectionNode
  | SelectionNode
  | SortNode;

class MemoryScan {
  idx: number;
  table: Array<Array<row>>;
  sorted: boolean = false;

  constructor(table: Array<Array<row>>) {
    this.table = table;
    this.idx = 0;
  }

  next() {
    if (this.idx >= this.table.length) {
      return null;
    }

    let x: any = this.table[this.idx];
    this.idx += 1;
    return x;
  }
}

class CSVFileScan {
  path: string;
  csv: Deno.FsFile | undefined;
  reader: ReadableStreamDefaultReader<string[]> | undefined
  done: boolean = false;
  
  // "/tmp/ml-20m/movies.csv"
  constructor(path: string) {
    this.path = path;
  }
  
  async next() {
    console.log("csv", this.csv)
    if (this.csv === undefined) {
      console.log("here", this.path)
      this.csv = await Deno.open(this.path, { read: true });
      
      this.reader = this.csv.readable
        .pipeThrough(new TextDecoderStream())
        .pipeThrough(new CsvParseStream())
        .getReader(); 
    } else {
      if (this.done) return null
      const result = await this.reader?.read()
      console.log("result", result)
      if (result?.done) {
        this.done = true;
        this.csv.close();
      } 
      return result?.value
    }
  }
}


class ProjectionNode {
  fn: projectionFunction;
  child: Nodeq | undefined;

  constructor(fn: projectionFunction) {
    this.fn = fn;
  }

  next() {
    const row = this.child?.next();

    if (row === null) return null;
    if (row.length === 0) return [];

    return this.fn(row);
  }
}

class SelectionNode {
  fn: selectionFunction;
  child: Nodeq;

  constructor(fn: selectionFunction) {
    this.fn = fn;
  }

  next() {
    let row = this.child.next();

    if (row === null) return null;
    if (row.length === 0) return [];
    if (this.fn(row)) return row;

    return [];
  }
}

class LimitNode {
  child: Nodeq;
  n: number;
  count: number;

  constructor(n: number) {
    this.n = n;
    this.count = 0;
  }

  async next() {
    const row = await this.child.next();
    console.log(row)
    if (row === null) return null;
    if (this.count === this.n) return null;
    if (row.length === 0) return [];

    this.count++;
    return row;
  }
}

class SortNode {
  sortFunc: sortFunction;
  sign: number = 1;
  child: MemoryScan | undefined;
  sortedRows: Array<Array<row>> = [];
  idx: number = 0;

  constructor(sortFunc: sortFunction, desc: boolean = false) {
    this.sortFunc = sortFunc;
    this.sign = desc ? 1 : -1; // js shenanigans
  }

  next() {
    if (this.idx === 0) this.sortMemory();

    if (this.idx >= this.sortedRows.length) return null;

    const r = this.sortedRows[this.idx];
    this.idx++;
    return r;
  }

  private sortMemory() {
    if (!this.sortedRows) {
      this.sortedRows = [];

      while (true) {
        const r = this.child?.next();
        if (!r) break;

        this.sortedRows.push(r);
      }
      this.sort();
    }
  }

  private sort() {
    this.sortedRows.sort((a, b) => {
      const field1 = this.sortFunc(a);
      const field2 = this.sortFunc(b);

      if (field1 < field2) return this.sign * 1;
      if (field1 > field2) return this.sign * -1;
      return 0;
    });
  }
}

const compareArrays = (a: row[][], b: row[][]): boolean => {
  if (a.length !== b.length) return false;

  a = a.sort();
  b = b.sort();
  for (let i = 0; i < a.length; i++) {
    for (let j = 0; j < a[i].length; j++) {
      if (a[i][j] !== b[i][j]) return false;
    }
  }

  return true;
};

function Q(nodes: Array<Nodeq>): Nodeq {
  const ns = nodes[Symbol.iterator]();
  const root = ns.next().value;
  let parent = root;

  for (const n of ns) {
    // if (typeof n !== MemoryScan) {
      parent.child = n;
      parent = n;
    // }
  }
  return root;
}

function* run(q: Nodeq) {
  while (true) {
    const x = q.next();

    if (Array.isArray(x) && x.length === 0) continue;
    if (!x) break;

    yield x;
  }
}

function main() {
  const birds = [
    ["amerob", "American Robin", 0.077, true],
    ["baleag", "Bald Eagle", 4.74, true],
    ["eursta", "European Starling", 0.082, true],
    ["barswa", "Barn Swallow", 0.019, true],
    ["ostric1", "Ostrich", 104.0, false],
    ["emppen1", "Emperor Penguin", 23.0, false],
    ["rufhum", "Rufous Hummingbird", 0.0034, true],
    ["comrav", "Common Raven", 1.2, true],
    ["wanalb", "Wandering Albatross", 8.5, false],
    ["norcar", "Northern Cardinal", 0.045, true],
  ];

  const schema = [
    ["id", String],
    ["name", String],
    ["weight", Number],
    ["in_us", Boolean],
  ];

  // const result1: row[][] = [
  //   ...run(
  //     Q([
  //       new ProjectionNode((row: row[]) => [row[1]]),
  //       new SelectionNode((row: row[]) => row[3] === false),
  //       new MemoryScan(birds),
  //     ]),
  //   ),
  // ];

  // console.assert(
  //   compareArrays(result1, [
  //     ["Ostrich"],
  //     ["Emperor Penguin"],
  //     ["Wandering Albatross"],
  //   ]),
  // );

  // const result2: row[][] = [
  //   ...run(
  //     Q([
  //       new ProjectionNode((row: row[]) => [row[0], row[2]]),
  //       new LimitNode(3),
  //       new SortNode((row: row[]) => row[2], true),
  //       new MemoryScan(birds),
  //     ]),
  //   ),
  // ];

  // console.assert(
  //   compareArrays(result2, [
  //     ["ostric1", 104.0],
  //     ["emppen1", 23.0],
  //     ["wanalb", 8.5],
  //   ]),
  // );

  const result3: row[][] = [
    ...run(
      Q([
        new LimitNode(3),
        new CSVFileScan("/tmp/ml-20m/movies.csv"),
      ]),
    ),
  ];
  
  console.log("her", result3)
  console.log("done");
}

main();

// async function main2() {
//   using csv = await Deno.open("/tmp/ml-20m/movies.csv", { read: true });
//   // const csvStream = new CsvStream(csv.readable);
//   const source = ReadableStream.from([
//     "name,age\n",
//     "Alice,34\n",
//     "Bob,24\n",
//   ]);
//   csv.readable
  
//   source
// const something = csv.readable.pipeThrough(new TextDecoderStream()).
//   pipeThrough(new CsvParseStream())
//   const reader = something.getReader();
//   let result = await reader.read()
//   console.log(result);
//   result = await reader.read()
//   console.log(result);
//   // console.log(`Read chunk: ${decoder.decode(result.value)}`)
  
// }

// main2();
