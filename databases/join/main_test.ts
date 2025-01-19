import {
  assert,
  assertArrayIncludes,
  assertEquals,
  assertObjectMatch,
} from "@std/assert";
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
} from "./nodes.ts";
import { CsvParseStream } from "jsr:@std/csv";
import { createTable } from "./util.ts";
import { defaultPageSize, HeapFile } from "./page.ts";
import { row } from "./type.ts";

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
      assertEquals(
        birdSchema.every((def, j) => value[def["name"]] === birds[i][j]),
        true
      );
      i++;
    }
  });

  await t.step("limit", () => {
    [...Array(birds.length + 2).keys()].map(async (i) => {
      const gen = run(Q([new Limit(i), new MemoryScan(birds, birdSchema)]));

      let idx = 0;
      for await (const value of gen) {
        assertEquals(
          birdSchema.every((def, j) => value[def["name"]] === birds[idx][j]),
          true
        );
        idx++;
      }
    });
  });

  await t.step("count", () => {
    [...Array(birds.length + 1).keys()].map(async (i) => {
      const gen = run(
        Q([new Count(), new Limit(i), new MemoryScan(birds, birdSchema)])
      );

      for await (const val of gen) {
        assertEquals(val["count"], i);
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
      assertEquals(
        birdSchema.every((def, j) => value[def["name"]] === birds[i][j]),
        true
      );
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
        new Selection((r) => r["in_us"] === false),
        new MemoryScan(birds, birdSchema),
      ])
    );

    let idx = 0;
    for await (const value of gen) {
      assertEquals(
        birdSchema.every((def, j) => value[def["name"]] === nonUSbirds[idx][j]),
        true
      );
      idx++;
    }
  });

  await t.step("projection", async () => {
    // Select everything
    let gen = run(
      Q([new Projection((r) => r), new MemoryScan(birds, birdSchema)])
    );

    let i = 0;
    for await (const value of gen) {
      assertEquals(
        birdSchema.every((def, j) => value[def["name"]] === birds[i][j]),
        true
      );
      i++;
    }

    // trivial projection
    gen = run(
      Q([
        new Projection(() => {
          return { hello: "world" } as row;
        }),
        new MemoryScan(birds, birdSchema),
      ])
    );

    i = 0;
    for await (const value of gen) {
      assertEquals(value["hello"], "world");
      i++;
    }

    // projection to two fields
    gen = run(
      Q([
        new Projection((r) => {
          return { id: r.id, in_us: r.in_us } as row;
        }),
        new MemoryScan(birds, birdSchema),
      ])
    );

    const expectations = birds.map((b) => {
      return { id: b[0], in_us: b[3] };
    });

    i = 0;
    for await (const value of gen) {
      assertEquals(Object.keys(value), ["id", "in_us"]);
      assertEquals(value["id"], expectations[i].id);
      assertEquals(value["in_us"], expectations[i].in_us);
      i++;
    }

    // selection then projection: names of non-us birds
    const birdNames = ["Ostrich", "Emperor Penguin", "Wandering Albatross"];

    gen = run(
      Q([
        new Projection((r) => {
          return { name: r.name } as row;
        }),
        new Selection((r) => r["in_us"] === false),
        new MemoryScan(birds, birdSchema),
      ])
    );

    for await (const value of gen) {
      assertArrayIncludes(birdNames, [value["name"]]);
    }

    // projection then selection
    gen = run(
      Q([
        new Selection((r) => r["in_us"] === false),
        new Projection((r) => {
          return { name: r.name, in_us: r.in_us } as row;
        }),
        new MemoryScan(birds, birdSchema),
      ])
    );

    for await (const value of gen) {
      assertArrayIncludes(birdNames, [value["name"]]);
      assertEquals(value["in_us"], false);
    }
  });

  await t.step("selection then projection", async () => {
    const gen = run(
      Q([
        new Projection((r) => {
          return { name: r.name } as row;
        }),
        new Selection((r) => r["in_us"] === false),
        new MemoryScan(birds, birdSchema),
      ])
    );

    const birdNames = ["Ostrich", "Emperor Penguin", "Wandering Albatross"];
    for await (const value of gen) {
      assertArrayIncludes(birdNames, [value["name"]]);
    }
  });

  await t.step("sort", async () => {
    const gen = run(
      Q([
        new Projection((r) => {
          return { id: r.id, weight: r.weight } as row;
        }),
        new Limit(3),
        new Sort("weight", true),
        new MemoryScan(birds, birdSchema),
      ])
    );

    const expectations = [
      { id: "ostric1", weight: 104.0 },
      { id: "emppen1", weight: 23.0 },
      { id: "wanalb", weight: 8.5 },
    ];

    let i = 0;
    for await (const value of gen) {
      assertEquals(value["id"], expectations[i].id);
      assertEquals(value["weight"], expectations[i].weight);
      i++;
    }
  });
});

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
      const constRowValue = Object.values(value);
      row.value?.every((c, i) => assertEquals(c, constRowValue[i]));
    }
  });
});

Deno.test("Test Insert and Scan", async (t) => {
  await t.step("create table and schema file", async () => {
    await createTable("test", testColumns, "test");
  });

  const file = new HeapFile("test", defaultPageSize, "test");
  await t.step("insert records and read back", async () => {
    const row = { a: 1, b: "bee", c: 1.0 };
    await file.insert([row]);

    let gen = run(Q([new DataFileScan("test", defaultPageSize, "test")]));
    for await (const value of gen) {
      assertObjectMatch(row, value);
    }

    // Write a subsequent and read BOTH back again
    const row2 = { a: 2, b: "see", c: 2.0 };
    await file.insert([row2]);
    gen = run(Q([new DataFileScan("test", defaultPageSize, "test")]));
    const output = [];
    for await (const value of gen) {
      output.push(value);
    }
    assertObjectMatch(row, output[0]);
    assertObjectMatch(row2, output[1]);

    gen = run(
      Q([new Count(), new DataFileScan("test", defaultPageSize, "test")])
    );

    for await (const val of gen) {
      assertEquals(val["count"], 2);
    }

    // - Write 100s of records (bulk insert) across a page boundary and count all
    const bulkRecords = [
      ...Array(1000)
        .keys()
        .map((i) => {
          return { a: i + 3, b: "etc", c: 0.0 };
        }),
    ];
    let i = 0;
    for await (const value of gen) {
      if (i === 0) assertObjectMatch(value, output[0]);
      if (i === 1) assertObjectMatch(value, output[1]);
      if (i > 1) assertObjectMatch(value, { a: i + 1, b: "etc", c: 0.0 });
      i++;
    }

    // - Write yet another record and count all
    await file.insert(bulkRecords);
    gen = run(
      Q([new Count(), new DataFileScan("test", defaultPageSize, "test")])
    );
    for await (const val of gen) {
      assertEquals(val["count"], 1002);
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
