import { Q, run } from "./util.ts";
import { DataFileScan, LimitNode } from "./nodes.ts";

async function main() {
  const gen = run(
    Q([
      new LimitNode(5),
      new DataFileScan("movies.data", 1024),
    ])
  );
  
  for await (const value of gen) {
    console.log(value);
  }
}

main();
