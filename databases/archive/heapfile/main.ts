import { Q, run, } from "./util.ts";
import { DataFileScan, Limit, Projection, Selection } from "./nodes.ts";

async function main() {
  const gen = run(
    Q([
      new Projection(["title", "genres"]),
      new Selection((row) => row["genres"] === "Adventure"),
      new Limit(2000),
      new DataFileScan("movies.data", 1024),
    ])
  );
  
  for await (const value of gen) {
    console.log(value);
  }
}

main();
