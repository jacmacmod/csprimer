import { CsvParseStream } from "@std/csv/parse-stream";

type row = string | number | boolean;
type Nodeq = CSVFileScan | LimitNode;

class CSVFileScan {
  path: string;
  csv: Deno.FsFile | undefined;
  reader: ReadableStreamDefaultReader<Record<string, string>>;
  
  constructor(path: string) {
    this.path = path;
  }

  async next() {
    if (this.csv === undefined) await this.initializeCsvReader();

    const result = await this.reader?.read();
    if (result?.done) {
      this.stop()
      return null;
    }
    return result.value && Object.values(result.value);
  }

  async initializeCsvReader() {
    this.csv = await Deno.open(this.path, { read: true });

    this.reader = this.csv.readable
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(
        new CsvParseStream({
          skipFirstRow: true,
        })
      )
      .getReader();
  }

  stop() {
    if (this.csv) this.csv.close();
 
    console.log("File closed.");
    return null;
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
    if (this.count === this.n) {
      this.child.stop();
      return null;
    }
    
    const row = await this.child.next();
    this.count++;
    
    if (row === null) return null;
    if (row.length === 0) return [];

    return row;
  }
}

function Q(nodes: Array<Nodeq>): Nodeq {
  const ns = nodes[Symbol.iterator]();
  const root = ns.next().value;
  let parent = root;

  for (const n of ns) {
    parent.child = n;
    parent = n;
  }
  return root;
}

async function* run(q: Nodeq) {
  while (true) {
    const x = await q.next();
    if (Array.isArray(x) && x.length === 0) continue;
    if (!x) break;

    yield await Promise.resolve(x);
  }
}

async function main() {
  const gen = run(
    Q([new LimitNode(3), new CSVFileScan("/tmp/ml-20m/movies.csv")])
  );
  
  for await (const value of gen) {
    console.log(value);
  }

  console.log("done");
}

main();
