import { Q, run } from "./util.ts";
import { DataFileScan, Limit, NestedLoopJoin, Count } from "./nodes.ts";

async function main() {
  
  const gen = run(
    Q([
      // new Selection((row) => row["genres"] === "Adventure"),
      new Count(),
      new Limit(3),
      new NestedLoopJoin("links"),
      new DataFileScan("movies"),
    ])
  );
  
  for await (const value of gen) {
    console.log(value);
  }
  
}

main();
