type row = string | number | boolean;
type projectionFunction = (arr: Array<row>) => Array<row>;
type selectionFunction = (arr: Array<row>) => boolean;
type sortFunction = (arr: Array<row>) => row;
type Nodeq =
  | CSVFileScan
  | Limit
  | MemoryScan
  | Projection
  | Selection
  | Sort;

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

class Projection {
  fn: projectionFunction;
  child: Nodeq;

  constructor(fn: projectionFunction) {
    this.fn = fn;
  }

  next() {
    let row = this.child.next();

    if (row === null) return null;
    if (row.length === 0) return [];

    return this.fn(row);
  }
}

class Selection {
  fn: selectionFunction;
  child: Nodeq;

  constructor(fn: selectionFunction) {
    this.fn = fn;
  }

  next() {
    const row = this.child.next();

    if (row === null) return null;
    if (row.length === 0) return [];
    if (this.fn(row)) return row;

    return [];
  }
}

class Limit {
  child: Nodeq;
  n: number;
  count: number;

  constructor(n: number) {
    this.n = n;
    this.count = 0;
  }

  next() {
    let row = this.child.next();

    if (row === null) return null;
    if (this.count === this.n) return null;
    if (row.length === 0) return [];

    this.count++;
    return row;
  }
}

class Sort {
  sortFunc: sortFunction;
  sign: number = 1;
  child: MemoryScan;
  sortedRows: Array<Array<row>>;
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
        let r = this.child.next();
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
  let ns = nodes[Symbol.iterator]();
  let root = ns.next().value;
  let parent = root;

  for (let n of ns) {
    parent.child = n;
    parent = n;
  }
  return root;
}

function* run(q: Nodeq) {
  while (true) {
    let x = q.next();

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

  const result1: row[][] = [
    ...run(
      Q([
        new Projection((row: row[]) => [row[1]]),
        new Selection((row: row[]) => row[3] === false),
        new MemoryScan(birds),
      ]),
    ),
  ];

  console.assert(
    compareArrays(result1, [
      ["Ostrich"],
      ["Emperor Penguin"],
      ["Wandering Albatross"],
    ]),
  );

  const result2: row[][] = [
    ...run(
      Q([
        new Projection((row: row[]) => [row[0], row[2]]),
        new Limit(3),
        new Sort((row: row[]) => row[2], true),
        new MemoryScan(birds),
      ]),
    ),
  ];

  console.assert(
    compareArrays(result2, [
      ["ostric1", 104.0],
      ["emppen1", 23.0],
      ["wanalb", 8.5],
    ]),
  );

}

main();
