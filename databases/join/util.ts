import * as path from "jsr:@std/path";

import { CSVFileScan, DataFileScan, MemoryScan, Nodeq } from "./nodes.ts";
import { rowItem } from "./type.ts";
import { defaultPageSize, defaultTableLocation, Page } from "./page.ts";
import { NestedLoopJoin } from "./nodes.ts";

type columnType = "int" | "text" | "float32" | "float64";

export type columnDefinition = {
  name: string;
  type: columnType;
  default?: string;
  nullable?: boolean;
};

export function Q(nodes: Array<Nodeq>): Nodeq {
  const ns = nodes[Symbol.iterator]();
  const root: Nodeq = ns.next().value!;
  let parent: Nodeq = root;

  for (const n of ns) {
    if (
      !(parent instanceof CSVFileScan) &&
      !(parent instanceof MemoryScan) &&
      !(parent instanceof NestedLoopJoin) &&
      !(parent instanceof DataFileScan) && 
      !(parent instanceof NestedLoopJoin)
    ) {
      parent.child = n;
      parent = n;
    }
  }
  return root;
}

export async function* run(q: Nodeq) {
  while (true) {
    const row = await q.next();
    if (row) {
      yield await Promise.resolve(row); 
    } else {
      break
    }
  }
}

export async function getSchema(
  table: string,
  dir: string = defaultTableLocation
): Promise<columnDefinition[]> {
  const result: columnDefinition[] = [];

  const data = await Deno.readFile(path.join(dir, `${table}_schema.data`));
  let bytesRead = 0;
  const decoder = new TextDecoder();

  while (bytesRead < data.length) {
    const column: columnDefinition = {
      name: "",
      nullable: false,
      type: "int",
    };

    const nameLength = data[bytesRead];
    bytesRead = bytesRead + 1;
    column.name = decoder.decode(data.slice(bytesRead, bytesRead + nameLength));
    bytesRead += nameLength;

    const typeLength = data[bytesRead];
    bytesRead = bytesRead + 1;
    column.type = decoder.decode(
      data.slice(bytesRead, bytesRead + typeLength)
    ) as columnType;
    bytesRead += typeLength;

    const bitfield = data[bytesRead];
    bytesRead++;
    if ((bitfield & 1) === 1) column.nullable = true;

    if ((bitfield & 2) === 2) {
      const defaultLength = data[bytesRead];
      bytesRead++;
      column.default = decoder.decode(data.slice(bytesRead, defaultLength));
      bytesRead += defaultLength;
    }

    result.push(column);
  }
  return result;
}

export async function createTable(
  name: string,
  columns: columnDefinition[],
  dir: string = defaultTableLocation
) {
  const file = await Deno.create(path.join(dir, `${name}_schema.data`));

  const output = [];
  let size = 0;
  const validTypes = ["text", "int", "float32", "float64"];

  for (const col of columns) {
    let totalBytes = 0;
    const buf = new Uint8Array(512);

    if (col["name"].length > 63) {
      console.log("column length is too long");
      return;
    }

    if (!validTypes.includes(col["type"])) {
      console.log("invalid column type");
      return;
    }

    if (col.default && col.default.length > 255) {
      console.log("default is too long", col["default"]);
      return;
    }

    if (col["default"] && col["type"] != "text") {
      console.log("only text types can have default values");
    }

    const encoder = new TextEncoder();

    buf.set([col["name"].length], 0);
    buf.set(encoder.encode(col["name"]), 1);
    totalBytes += col["name"].length + 1;

    buf.set([col["type"].length], totalBytes);
    buf.set(encoder.encode(col["type"]), totalBytes + 1);
    totalBytes += col["type"].length + 1;

    let bitField = 0;

    if (col["nullable"]) bitField |= 1;
    if (col["default"]) {
      bitField |= 2;

      buf.set([bitField], totalBytes);
      totalBytes += 1;

      buf.set([col["default"].length], totalBytes);
      totalBytes += 1;
      buf.set(encoder.encode(col["default"]), totalBytes);
      totalBytes += col["default"].length;
    } else {
      buf.set([bitField], totalBytes);
      totalBytes += 1;
    }
    size += totalBytes;
    output.push(buf.slice(0, totalBytes));
  }
  const result = new Uint8Array(size);
  let offset = 0;

  for (const arr of output) {
    result.set(arr, offset);
    offset += arr.length;
  }

  await file.write(result);
  file.close();
  await Deno.create(path.join(dir, `${name}.data`)).then((f) => f.close());

  console.log(`created ${name} table`);
}

export async function insertCSV(
  loc: string,
  tableName: string,
  pageSize: number = defaultPageSize,
  dir: string = defaultTableLocation
): Promise<number> {
  console.time(`${tableName} insertion time`);

  const columnAttributes = await getSchema(tableName, dir);
  if (columnAttributes.length === 0) Deno.exit(1);

  const rows = run(Q([new CSVFileScan(loc, tableName, dir)]));

  const tableFileLocation = path.join(dir, `${tableName}.data`);
  const file = await Deno.open(tableFileLocation, { write: true });
  const writer = file.writable.getWriter();

  let pageNumber: number = 1;
  let page = new Page(pageNumber, pageSize);
  let totalEntries = 0;

  for await (const row of rows) {
    const encodedRow = encodeRow(row, columnAttributes);
    totalEntries++;

    // create at least 2 bytes of cushion
    if (page.lowerOffset - page.upperOffset < encodedRow.length + 6) {
      await writer.write(page.createBuffer());
      pageNumber++;
      page = new Page(pageNumber, pageSize);
    }
    page.addRow(encodedRow);
  }

  await writer.write(page.createBuffer());
  await writer.close();

  console.log(`inserted ${totalEntries} entries into ${tableName} table`);
  console.timeEnd(`${tableName} insertion time`);

  return totalEntries;
}

export function encodeRow(
  row: rowItem[],
  schema: columnDefinition[]
): Uint8Array {
  const output = [];
  let size = 0;

  for (let i = 0; i < schema.length; i++) {
    const [value, type] = [row[i], schema[i].type];
    let encodedColumn: Uint8Array;

    switch (type) {
      case "int":
        encodedColumn = encodeUint32(Number(value));
        break;
      case "text":
        encodedColumn = encodeString(String(value));
        break;
      case "float32":
      case "float64": {
        const parsedValue =
          typeof value === "string" ? parseFloat(value) : Number(value);

        if (isNaN(parsedValue)) {
          console.log("Unable to parse float");
          Deno.exit(1);
        }
        encodedColumn =
          type === "float32"
            ? encodeFloat32(parsedValue)
            : encodeFloat64(parsedValue);

        break;
      }
      default:
        console.log("Unable to find column type match");
        Deno.exit(1);
    }

    output.push(encodedColumn);
    size += encodedColumn.length;
  }

  const result = new Uint8Array(size);
  let offset = 0;

  for (const arr of output) {
    result.set(arr, offset);
    offset += arr.length;
  }

  return result;
}

export function decodeRow(
  arr: Uint8Array,
  schema: columnDefinition[]
): rowItem[] {
  const row: rowItem[] = [];
  let offset = 0;

  for (let i = 0; i < schema.length; i++) {
    switch (schema[i].type) {
      case "int": {
        const intColumn = decodeUint32(arr.slice(offset, offset + 4));
        row.push(intColumn);
        offset += 4;
        break;
      }
      case "text": {
        const textColumn = decodeString(arr, offset);
        offset += arr[offset] + 1;
        row.push(textColumn);
        break;
      }
      case "float32": {
        const float32Column = decodeFloat32(arr.slice(offset, offset + 4));
        row.push(float32Column);
        offset += 4;
        break;
      }
      case "float64": {
        const float64Column = decodeFloat64(arr.slice(offset, offset + 8));
        row.push(float64Column);
        offset += 8;
        break;
      }
      default:
        console.log("Unable to find column type match");
        Deno.exit(1);
    }
  }

  return row;
}

function encodeString(str: string): Uint8Array {
  if (str.length > 255) {
    console.error("string too long");
  }
  const textEncoder = new TextEncoder();
  const encoded = textEncoder.encode(str);

  return new Uint8Array([encoded.length, ...encoded]);
}

function decodeString(arr: Uint8Array, offset: number): string {
  const decoder = new TextDecoder();
  const startIdx = offset + 1;
  const textBytes = arr.slice(startIdx, startIdx + arr[offset]);

  return decoder.decode(textBytes);
}

export function encodeUint16(n: number): Uint8Array {
  const buffer = new ArrayBuffer(2);
  const view = new DataView(buffer);
  view.setUint16(0, n, true);

  return new Uint8Array(view.buffer);
}

export function decodeUint16(arr: Uint8Array): number {
  const view = new DataView(arr.buffer);

  return view.getUint16(0, true);
}

function encodeUint32(n: number): Uint8Array {
  const buffer = new ArrayBuffer(4);
  const view = new DataView(buffer);
  view.setUint32(0, n, true);

  return new Uint8Array(view.buffer);
}

export function decodeUint32(arr: Uint8Array): number {
  const view = new DataView(arr.buffer);

  return view.getUint32(0, true);
}

function encodeFloat32(n: number): Uint8Array {
  const buffer = new ArrayBuffer(4);
  const view = new DataView(buffer);
  view.setFloat32(0, n, true);

  return new Uint8Array(view.buffer);
}

export function decodeFloat32(arr: Uint8Array): number {
  const view = new DataView(arr.buffer);

  return view.getFloat32(0, true);
}

function encodeFloat64(n: number): Uint8Array {
  const buffer = new ArrayBuffer(8);
  const view = new DataView(buffer);
  view.setFloat64(0, n, true);

  return new Uint8Array(view.buffer);
}

export function decodeFloat64(arr: Uint8Array): number {
  const view = new DataView(arr.buffer);

  return view.getFloat64(0, true);
}
