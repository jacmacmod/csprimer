import { assertArrayIncludes, assertEquals } from "@std/assert";
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
  HashJoin,
  MergeJoin,
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
      assertArrayIncludes(value, birds[i]);
      i++;
    }
  });

  await t.step("limit", () => {
    [...Array(birds.length + 2).keys()].map(async (i) => {
      const gen = run(Q([new Limit(i), new MemoryScan(birds, birdSchema)]));

      const result = [];
      for await (const value of gen) {
        result.push(value);
      }
      result.forEach((r, idx) => assertArrayIncludes(r, birds[idx]));
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
        new Sort((r: row) => r[2], true),
        new MemoryScan(birds, birdSchema),
      ])
    );

    const expectations = [
      ["ostric1", 104.0],
      ["emppen1", 23.0],
      ["wanalb", 8.5],
    ];

    let i = 0;
    for await (const value of gen) {
      assertEquals(value[0], expectations[i][0]);
      assertEquals(value[1], expectations[i][1]);
      i++;
    }
  });
});

Deno.test("NestedLoopJoin", async (t) => {
  const schema: columnDefinition[] = [
    {
      name: "a",
      type: "int",
    },
    {
      name: "b",
      type: "text",
    },
  ];

  const tableA = [
    [1, "a"],
    [2, "b"],
  ];

  const tableB = [
    [3, "A"],
    [4, "B"],
    [5, "C"],
  ];

  await t.step("self join", async () => {
    const gen = run(
      Q([
        new NestedLoopJoin(
          new MemoryScan(tableA, schema),
          new MemoryScan(tableA, schema)
        ),
      ])
    );

    const result = [];
    for await (const value of gen) {
      result.push(value);
    }

    assertArrayIncludes(result[0], [1, "a", 1, "a"]);
    assertArrayIncludes(result[1], [1, "a", 2, "b"]);
    assertArrayIncludes(result[2], [2, "b", 1, "a"]);
    assertArrayIncludes(result[3], [2, "b", 2, "b"]);
  });

  await t.step("join A X B", async () => {
    const gen = run(
      Q([
        new NestedLoopJoin(
          new MemoryScan(tableA, schema),
          new MemoryScan(tableB, schema)
        ),
      ])
    );

    const result = [];
    for await (const value of gen) {
      result.push(value);
    }

    assertArrayIncludes(result[0], [1, "a", 3, "A"]);
    assertArrayIncludes(result[1], [1, "a", 4, "B"]);
    assertArrayIncludes(result[2], [1, "a", 5, "C"]);
    assertArrayIncludes(result[3], [2, "b", 3, "A"]);
    assertArrayIncludes(result[4], [2, "b", 4, "B"]);
    assertArrayIncludes(result[5], [2, "b", 5, "C"]);
  });

  await t.step("select after join", async () => {
    const gen = run(
      Q([
        new Selection((r) => r[3] === "b"),
        new NestedLoopJoin(
          new MemoryScan(tableA, schema),
          new MemoryScan(tableA, schema)
        ),
      ])
    );

    const result = [];
    for await (const value of gen) {
      result.push(value);
    }

    assertArrayIncludes(result[0], [1, "a", 2, "b"]);
    assertArrayIncludes(result[1], [2, "b", 2, "b"]);
  });

  await t.step("select before join", async () => {
    const selection = new Selection((r) => r[1] === "b");
    selection.child = new MemoryScan(tableA, schema);
    const gen = run(
      Q([new NestedLoopJoin(selection, new MemoryScan(tableA, schema))])
    );

    const result = [];
    for await (const value of gen) {
      result.push(value);
    }

    assertArrayIncludes(result[0], [2, "b", 1, "a"]);
    assertArrayIncludes(result[1], [2, "b", 2, "b"]);
  });

  await t.step("three-way self join", async () => {
    const gen = run(
      Q([
        new NestedLoopJoin(
          new NestedLoopJoin(
            new MemoryScan(tableA, schema),
            new MemoryScan(tableA, schema)
          ),
          new MemoryScan(tableA, schema)
        ),
      ])
    );

    const result = [];
    for await (const value of gen) {
      result.push(value);
    }

    assertArrayIncludes(result[0], [1, "a", 1, "a", 1, "a"]);
    assertArrayIncludes(result[1], [1, "a", 2, "b", 1, "a"]);
    assertArrayIncludes(result[2], [2, "b", 1, "a", 1, "a"]);
    assertArrayIncludes(result[3], [2, "b", 2, "b", 1, "a"]);

    assertArrayIncludes(result[4], [1, "a", 1, "a", 2, "b"]);
    assertArrayIncludes(result[5], [1, "a", 2, "b", 2, "b"]);
    assertArrayIncludes(result[6], [2, "b", 1, "a", 2, "b"]);
    assertArrayIncludes(result[7], [2, "b", 2, "b", 2, "b"]);
  });

  await t.step("join two data files -- movielens", async () => {
    const gen = run(
      Q([
        new Projection((r) => [r[0], r[1], r[2], r[4], r[5]]),
        new Limit(2),
        new Selection((r) => r[0] === r[3]),
        new NestedLoopJoin(
          new DataFileScan("movies", defaultPageSize, "tables"),
          new DataFileScan("links", defaultPageSize, "tables")
        ),
      ])
    );

    const result = [];
    for await (const value of gen) {
      result.push(value);
    }

    assertArrayIncludes(result[0], [
      1,
      "Toy Story (1995)",
      "Adventure|Animation|Children|Comedy|Fantasy",
      "0114709",
      862,
    ]);
    assertArrayIncludes(result[1], [
      2,
      "Jumanji (1995)",
      "Adventure|Children|Fantasy",
      "0113497",
      8844,
    ]);
  });
});

Deno.test("Hash join", async (t) => {
  await t.step("movies X links: join ON movieID Limit 10", async () => {
    const expectations = [
      [
        1,
        "Toy Story (1995)",
        "Adventure|Animation|Children|Comedy|Fantasy",
        "0114709",
        862,
      ],
      [2, "Jumanji (1995)", "Adventure|Children|Fantasy", "0113497", 8844],
      [3, "Grumpier Old Men (1995)", "Comedy|Romance", "0113228", 15602],
      [4, "Waiting to Exhale (1995)", "Comedy|Drama|Romance", "0114885", 31357],
      [5, "Father of the Bride Part II (1995)", "Comedy", "0113041", 11862],
      [6, "Heat (1995)", "Action|Crime|Thriller", "0113277", 949],
      [7, "Sabrina (1995)", "Comedy|Romance", "0114319", 11860],
      [8, "Tom and Huck (1995)", "Adventure|Children", "0112302", 45325],
      [9, "Sudden Death (1995)", "Action", "0114576", 9091],
      [10, "GoldenEye (1995)", "Action|Adventure|Thriller", "0113189", 710],
    ];

    const gen = run(
      Q([
        new Projection((r) => [r[0], r[1], r[2], r[4], r[5]]),
        new Limit(10),
        new HashJoin(
          new DataFileScan("movies", defaultPageSize, "tables"),
          new DataFileScan("links", defaultPageSize, "tables"),
          (r: row) => r[0],
          (r: row) => r[0]
        ),
      ])
    );

    let i = 0;
    for await (const value of gen) {
      assertArrayIncludes(value, expectations[i]);
      i++;
    }
  });

  await t.step("movies X ratings: join ON movieID Limit 20", async () => {
    const expectations = [
      [2, "Jumanji (1995)", 1, 2, 3.5],
      [
        29,
        "City of Lost Children, The (Cité des enfants perdus, La) (1995)",
        1,
        29,
        3.5,
      ],
      [32, "Twelve Monkeys (a.k.a. 12 Monkeys) (1995)", 1, 32, 3.5],
      [47, "Seven (a.k.a. Se7en) (1995)", 1, 47, 3.5],
      [50, "Usual Suspects, The (1995)", 1, 50, 3.5],
      [112, "Rumble in the Bronx (Hont faan kui) (1995)", 1, 112, 3.5],
      [151, "Rob Roy (1995)", 1, 151, 4],
      [223, "Clerks (1994)", 1, 223, 4],
      [
        253,
        "Interview with the Vampire: The Vampire Chronicles (1994)",
        1,
        253,
        4,
      ],
      [260, "Star Wars: Episode IV - A New Hope (1977)", 1, 260, 4],
      [
        293,
        "Léon: The Professional (a.k.a. The Professional) (Léon) (1994)",
        1,
        293,
        4,
      ],
      [296, "Pulp Fiction (1994)", 1, 296, 4],
      [318, "Shawshank Redemption, The (1994)", 1, 318, 4],
      [337, "What's Eating Gilbert Grape (1993)", 1, 337, 3.5],
      [367, "Mask, The (1994)", 1, 367, 3.5],
      [541, "Blade Runner (1982)", 1, 541, 4],
      [589, "Terminator 2: Judgment Day (1991)", 1, 589, 3.5],
      [593, "Silence of the Lambs, The (1991)", 1, 593, 3.5],
      [653, "Dragonheart (1996)", 1, 653, 3],
      [919, "Wizard of Oz, The (1939)", 1, 919, 3.5],
    ];

    const gen = run(
      Q([
        new Projection((r) => [r[0], r[1], r[3], r[4], r[5]]),
        new Limit(20),
        new HashJoin(
          new DataFileScan("movies", defaultPageSize, "tables"),
          new DataFileScan("ratings", defaultPageSize, "tables"),
          (r: row) => r[0],
          (r: row) => r[1]
        ),
      ])
    );

    let i = 0;
    for await (const value of gen) {
      assertArrayIncludes(value, expectations[i]);
      i++;
    }
  });
});

Deno.test("MergeJoin", async (t) => {
  await t.step("unique left join relation condition", async () => {
    const userSchema: columnDefinition[] = [
      {
        name: "userId",
        type: "int",
      },
      {
        name: "name",
        type: "text",
      },
    ];

    const paymentSchema: columnDefinition[] = [
      {
        name: "paymentId",
        type: "int",
      },
      {
        name: "userId",
        type: "int",
      },
      {
        name: "amount",
        type: "float64",
      },
    ];

    const users = [
      [2, "bar"],
      [1, "foo"],
    ];

    const payments = [
      [1, 2, 0.5],
      [2, 1, 0.7],
      [3, 2, 0.9],
    ];

    const left = new Sort((r) => r[0]);
    left.child = new MemoryScan(users, userSchema);

    const right = new Sort((r) => r[1]);
    right.child = new MemoryScan(payments, paymentSchema);

    const gen = run(
      Q([
        new MergeJoin(
          left,
          right,
          (r: row) => r[0],
          (r: row) => r[1]
        ),
      ])
    );

    const result = [];
    for await (const value of gen) {
      result.push(value);
    }
    console.log(result)
    
    assertArrayIncludes(result[0], [1, "foo", 2, 1, 0.7]);
    assertArrayIncludes(result[1], [2, "bar", 1, 2, 0.5]);
    assertArrayIncludes(result[2], [2, "bar", 3, 2, 0.9]);
  });

  await t.step("non-unique left join relation condition", async () => {
    const userSchema: columnDefinition[] = [
      {
        name: "userId",
        type: "int",
      },
      {
        name: "groupId",
        type: "int",
      },
      {
        name: "name",
        type: "text",
      },
    ];

    const paymentSchema: columnDefinition[] = [
      {
        name: "paymentId",
        type: "int",
      },
      {
        name: "groupId",
        type: "int",
      },
      {
        name: "amount",
        type: "float64",
      },
    ];
    const left = [
      [1, 2, "foo"],
      [2, 1, "bar"],
      [3, 1, "baz"],
    ];


    const right = [
      [1, 1, 0.2],
      [2, 1, 0.3],
      [3, 3, 0.4],
    ];

    const leftRelation = new Sort((r: row) => r[1]);
    leftRelation.child = new MemoryScan(left, userSchema);

    const rightRelation = new Sort((r: row) => r[1]);
    rightRelation.child = new MemoryScan(right, paymentSchema);

    const gen = run(
      Q([
        new MergeJoin(
          leftRelation,
          rightRelation,
          (r: row) => r[1],
          (r: row) => r[1]
        ),
      ])
    );

    const result = [];
    for await (const value of gen) {
      result.push(value);
    }
    console.log(result)
    assertArrayIncludes(result[0], [2, 1, "bar", 1, 1, 0.2]);
    assertArrayIncludes(result[1], [2, 1, "bar", 2, 1, 0.3]);
    assertArrayIncludes(result[2], [3, 1, "baz", 1, 1, 0.2]);
    assertArrayIncludes(result[3], [3, 1, "baz", 2, 1, 0.3]);
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

Deno.test("csv file scan", async (t) => {
  await t.step("create table and schema file", async () => {
    await createTable("test", testColumns, "test");
  });

  await t.step("compare csv rows and csv file scan output", async () => {
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

Deno.test("test data scan", async (t) => {
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
