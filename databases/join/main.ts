import { Q, run } from "./util.ts";
import { DataFileScan, LimitNode, NestedLoopJoin, CountNode } from "./nodes.ts";

async function main() {
  
  const gen = run(
    Q([
      // new SelectionNode((row) => row["genres"] === "Adventure"),
      new CountNode(),
      new LimitNode(3),
      new NestedLoopJoin("links"),
      new DataFileScan("movies"),
    ])
  );
  
  for await (const value of gen) {
    console.log(value);
  }
  
}

main();
