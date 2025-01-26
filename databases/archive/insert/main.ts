import { Q, run } from "./util.ts";
import { Count, DataFileScan, Limit, Selection } from "./nodes.ts";
import { HeapFile } from "./page.ts";
import { row } from "./type.ts";

async function main() {
  let id = 131280;
  const moviesTable = new HeapFile("movies.data", 1024);
  const rows = [];
  for (let i = 1; i <= 25; i++) {
    const row: row = {
      title: "Jack's favorite movie in the world # " + String(i),
      genres: "Action",
      movieId: i,
    };
    rows.push(row);
  }
  moviesTable.insert(rows);

  const gen = run(
    Q([
      // new Selection(
      //   (row) => row["genres"] === "Action" && row["title"] === "Heat",
      // ),
      // new Limit(1),
      new Count(),
      new DataFileScan("movies.data", 1024),
    ]),
  );

  for await (const value of gen) {
    console.log(value);
  }
}

main();
