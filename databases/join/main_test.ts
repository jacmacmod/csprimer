import { assertEquals, assertObjectMatch } from "@std/assert";
import { run, Q, decodeRow, columnDefinition, insertCSV, encodeRow, getSchema } from "./util.ts";
import { CSVFileScan, DataFileScan } from "./nodes.ts";
import { CsvParseStream } from "jsr:@std/csv";
import { createTable } from "./util.ts";
import { defaultPageSize } from "./page.ts";

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

Deno.test(async function csvScanTest() {
  createTable("test", testColumns, "test");

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

Deno.test(async function dataFileScanTest() {
  await createTable("test", testColumns, "test");
  await insertCSV("test/test.csv", "test", defaultPageSize, "test")

  const gen = run(Q([new DataFileScan("test", defaultPageSize, "test")]));
  for await (const value of gen) {
    assertEquals(value, decodeRow(encodeRow(value, testColumns), testColumns))
  }
});
