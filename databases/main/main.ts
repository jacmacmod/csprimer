import { Q, run } from "./util.ts";
import { DataFileScan, Limit, HashJoin, Projection } from "./nodes.ts";
import { defaultPageSize } from "./page.ts";

async function main() {
  const gen = run(
    Q([
      new Projection((r) => [r[0], r[1], r[3], r[4],r[5]]),
      new Limit(20),
      new HashJoin(
        new DataFileScan("movies", defaultPageSize, "tables"),
        new DataFileScan("ratings", defaultPageSize, "tables"),
        0,
        1
      ),
    ])
  );

  for await (const value of gen) {
    console.log(value);
  }
}

main();
