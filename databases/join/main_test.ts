import { assertAlmostEquals, assertArrayIncludes, assertEquals } from "@std/assert";
import {
  run,
  Q,
  decodeRow,
  columnDefinition,
  insertCSV,
  encodeRow,
} from "./util.ts";
import {
  CSVFileScan,
  DataFileScan,
  Limit,
  MemoryScan,
  Projection,
  Selection,
  Count,
  Sort,
  NestedLoopJoin,
} from "./nodes.ts";
import { CsvParseStream } from "jsr:@std/csv";
import { createTable } from "./util.ts";
import { defaultPageSize, HeapFile } from "./page.ts";

Deno.test("inMemory", async (t) => {
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

  const birdSchema: columnDefinition[] = [
    {
      name: "id",
      type: "text",
    },
    {
      name: "name",
      type: "text",
    },
    {
      name: "weight",
      type: "float64",
    },
    {
      name: "in_us",
      type: "text",
    },
  ];

  await t.step("memory scan", async () => {
    const gen = run(Q([new MemoryScan(birds, birdSchema)]));
    let i = 0;

    for await (const value of gen) {
      assertArrayIncludes(value, birds[i]);
      i++;
    }
  });

  await t.step("limit", () => {
    [...Array(birds.length + 2).keys()].map(async (i) => {
      const gen = run(Q([new Limit(i), new MemoryScan(birds, birdSchema)]));

      const result = [];
      for await (const value of gen) {
        result.push(value)
      }
      result.forEach((r, idx) => assertArrayIncludes(r, birds[idx]))
    });
  });

  await t.step("count", () => {
    [...Array(birds.length + 1).keys()].map(async (i) => {
      const gen = run(
        Q([new Count(), new Limit(i), new MemoryScan(birds, birdSchema)])
      );

      for await (const val of gen) {
        assertEquals(val[0], i);
      }
    });
  });

  await t.step("selection", async () => {
    // Select everything
    let gen = run(
      Q([new Selection(() => true), new MemoryScan(birds, birdSchema)])
    );

    let i = 0;
    for await (const value of gen) {
      assertArrayIncludes(value, birds[i]);
      i++;
    }

    // Select nothing
    gen = run(
      Q([new Selection(() => false), new MemoryScan(birds, birdSchema)])
    );

    const result = await gen.next();
    assertEquals(result.value, undefined);

    // select non-us birds
    const nonUSbirds = [
      ["ostric1", "Ostrich", 104.0, false],
      ["emppen1", "Emperor Penguin", 23.0, false],
      ["wanalb", "Wandering Albatross", 8.5, false],
    ];

    gen = run(
      Q([
        new Selection((r) => r[3] === false),
        new MemoryScan(birds, birdSchema),
      ])
    );

    i = 0;
    for await (const value of gen) {
      assertArrayIncludes(value, nonUSbirds[i]);
      i++;
    }
  });

  await t.step("projection", async () => {
    // Select everything
    let gen = run(
      Q([new Projection((r) => r), new MemoryScan(birds, birdSchema)])
    );

    let i = 0;
    for await (const value of gen) {
      assertArrayIncludes(value, birds[i]);
      i++;
    }

    // trivial projection
    gen = run(
      Q([new Projection(() => ["hello"]), new MemoryScan(birds, birdSchema)])
    );

    i = 0;
    for await (const value of gen) {
      assertEquals(value, ["hello"]);
      i++;
    }

    // projection to two fields
    gen = run(
      Q([
        new Projection((r) => [r[0], r[3]]),
        new MemoryScan(birds, birdSchema),
      ])
    );

    const expectations = birds.map((b) => [b[0], b[3]]);

    i = 0;
    for await (const value of gen) {
      assertEquals(value[0], expectations[i][0]);
      assertEquals(value[1], expectations[i][1]);
      i++;
    }

    // selection then projection: names of non-us birds
    const birdNames = ["Ostrich", "Emperor Penguin", "Wandering Albatross"];

    gen = run(
      Q([
        new Projection((r) => [r[1]]),
        new Selection((r) => r[3] === false),
        new MemoryScan(birds, birdSchema),
      ])
    );

    for await (const value of gen) {
      assertArrayIncludes(birdNames, [value[0]]);
    }

    // projection then selection
    gen = run(
      Q([
        new Selection((r) => r[3] === false),
        new Projection((r) => [r[1], r[3]]),
        new MemoryScan(birds, birdSchema),
      ])
    );

    for await (const value of gen) {
      assertArrayIncludes(birdNames, [value[0]]);
      assertEquals(value[1], false);
    }
  });

  await t.step("selection then projection", async () => {
    const gen = run(
      Q([
        new Projection((r) => [r[1]]),
        new Selection((r) => r[3] === false),
        new MemoryScan(birds, birdSchema),
      ])
    );

    const birdNames = ["Ostrich", "Emperor Penguin", "Wandering Albatross"];
    for await (const value of gen) {
      assertArrayIncludes(birdNames, [value[0]]);
    }
  });

  await t.step("sort", async () => {
    const gen = run(
      Q([
        new Projection((r) => [r[0], r[2]]),
        new Limit(3),
        new Sort(2, true),
        new MemoryScan(birds, birdSchema),
      ])
    );

    const expectations = [
     [ "ostric1", 104.0 ],
     [ "emppen1", 23.0 ],
     [ "wanalb", 8.5 ],
    ];

    let i = 0;
    for await (const value of gen) {
      assertEquals(value[0], expectations[i][0]);
      assertEquals(value[1], expectations[i][1]);
      i++;
    }
  });
});

// Deno.test("NestedLoopJoin", async (t) => {
//   const tableASchema: columnDefinition[] = [
//     {
//       name: "a",
//       type: "int",
//     },
//     {
//       name: "b",
//       type: "text",
//     },
//   ];

//   const tableBSchema: columnDefinition[] = [
//     {
//       name: "B",
//       type: "int",
//     },
//     {
//       name: "C",
//       type: "text",
//     },
//   ];
//   const tableA = [
//     [1, "a"],
//     [2, "b"],
//   ];
//   const tableB = [
//     [2, "A"],
//     [3, "B"],
//   ];
//   await t.step("self join", async () => {
//     const gen = run(
//       Q([
//         new NestedLoopJoin(
//           new MemoryScan(tableA, tableASchema),
//           new MemoryScan(tableA, tableASchema)
//         ),
//       ])
//     );

//     let i = 0;
//     const result: any[] = [];
//     for await (const value of gen) {
//       result.push(value);
//       // assertEquals(value["id"], expectations[i].id);
//       i++;
//     }
//     assertArrayIncludes(result[0], [1, "a", 1, "a"]);
//     assertArrayIncludes(result[1], [1, "a", 2, "b"]);
//     assertArrayIncludes(result[2], [2, "b", 1, "a"]);
//     assertArrayIncludes(result[3], [2, "b", 1, "a"]);
//     // assertEquals(value["weight"], expectations[i].weight);
//     // A againt A
//   });
//   await t.step("select after  join", async () => {
//     // select those where one field matches another
//   });
//   await t.step("test select before  join", async () => {
//     // child should not have to memory scan?
//   });
//   await t.step("three way self join", async () => {
//     // A x A x A
//   });
// });

const testColumns: columnDefinition[] = [
  {
    name: "a",
    type: "int",
  },
  {
    name: "b",
    type: "text",
  },
  {
    name: "c",
    type: "float64",
  },
];

Deno.test("Test CSV file scan", async (t) => {
  await t.step("create table and schema file", async () => {
    await createTable("test", testColumns, "test");
  });

  await t.step("Test CSV scan", async () => {
    const gen = run(Q([new CSVFileScan("test/test.csv", "test", "test")]));

    const csv = await Deno.open("test/test.csv", { read: true });

    const reader = csv.readable
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(new CsvParseStream())
      .getReader();

    await reader.read();

    for await (const value of gen) {
      const row = await reader.read();
      value.every((c, i) => assertEquals(c, row.value![i]));
    }
  });
});

Deno.test("Test Insert and Scan", async (t) => {
  await t.step("create table and schema file", async () => {
    await createTable("test", testColumns, "test");
  });

  const file = new HeapFile("test", defaultPageSize, "test");
  await t.step("insert records and read back", async () => {
    const row = [1, "bee", 1.0];
    await file.insert([row]);

    let gen = run(Q([new DataFileScan("test", defaultPageSize, "test")]));
    for await (const value of gen) {
      assertArrayIncludes(row, value);
    }

    // Write a subsequent and read BOTH back again
    const row2 = [2, "see", 2.0];
    await file.insert([row2]);
    gen = run(Q([new DataFileScan("test", defaultPageSize, "test")]));
    const output = [];
    for await (const value of gen) {
      output.push(value);
    }
    assertArrayIncludes(row, output[0]);
    assertArrayIncludes(row2, output[1]);

    gen = run(
      Q([new Count(), new DataFileScan("test", defaultPageSize, "test")])
    );

    for await (const val of gen) {
      assertEquals(val[0], 2);
    }

    // - Write 100s of records (bulk insert) across a page boundary and count all
    const bulkRecords = [
      ...Array(1000)
        .keys()
        .map((i) => [i + 3, "etc", 0.0]),
    ];
    let i = 0;
    for await (const value of gen) {
      if (i === 0) assertArrayIncludes(value, output[0]);
      if (i === 1) assertArrayIncludes(value, output[1]);
      if (i > 1) assertArrayIncludes(value, [i + 1, "etc", 0.0]);
      i++;
    }

    // - Write yet another record and count all
    await file.insert(bulkRecords);
    gen = run(
      Q([new Count(), new DataFileScan("test", defaultPageSize, "test")])
    );
    for await (const val of gen) {
      assertEquals(val[0], 1002);
    }
  });

  await t.step("insert csv", async () => {
    const entries = await insertCSV(
      "test/test.csv",
      "test",
      defaultPageSize,
      "test"
    );
    assertEquals(entries, 5);
  });

  await t.step("round trip encode/decode test", async () => {
    const gen = run(Q([new DataFileScan("test", defaultPageSize, "test")]));
    for await (const value of gen) {
      assertEquals(
        value,
        decodeRow(encodeRow(value, testColumns), testColumns)
      );
    }
  });
});
