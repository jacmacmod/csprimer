import { Q, run, } from "./util.ts";
import { DataFileScan, LimitNode, ProjectionNode, SelectionNode } from "./nodes.ts";

async function main() {
  const gen = run(
    Q([
      new ProjectionNode(["title", "genres"]),
      new SelectionNode((row) => row["genres"] === "Adventure"),
      new LimitNode(2000),
      new DataFileScan("movies.data", 1024),
    ])
  );
  
  for await (const value of gen) {
    console.log(value);
  }
}

main();
