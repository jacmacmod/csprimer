import { rowItem } from "./type.ts";
import {
  columnDefinition,
  decodeRow,
  encodeRow,
  getSchema,
  decodeUint16,
  encodeUint16,
} from "./util.ts";

import * as path from "jsr:@std/path";

export const defaultPageSize = 1024;
export const defaultTableLocation = "tables";

export class Page {
  id: number;
  pageSize: number;

  upperOffset: number = 6;
  lowerOffset: number;

  rowOffsets: number[] = [];
  rows: Uint8Array[] = [];

  constructor(pageId: number, pageSize: number = defaultPageSize) {
    this.id = pageId;
    this.pageSize = pageSize;
    this.lowerOffset = this.pageSize;
  }

  addRow(row: Uint8Array) {
    this.rows?.push(row);
    this.upperOffset += 2;
    this.lowerOffset -= row.length;
    this.rowOffsets.push(this.lowerOffset);
  }

  createBuffer(): Uint8Array {
    // pageid (Uint16) lower(Uint16) upper(Uint16) (rowItem(Uint16)...n items))
    const buffer = new ArrayBuffer(this.pageSize);
    const view = new DataView(buffer);

    view.setUint16(0, this.id, true);
    view.setUint16(2, this.lowerOffset, true);
    view.setUint16(4, this.upperOffset, true);

    let currentOffset = 6;
    this.rowOffsets.forEach((ro) => {
      view.setUint16(currentOffset, ro, true);
      currentOffset += 2;
    });

    const output = new Uint8Array(view.buffer);

    let offset = this.lowerOffset;
    for (let i = this.rows.length - 1; i >= 0; i--) {
      output.set(this.rows[i], offset);
      offset += this.rows[i].length;
    }

    return output;
  }
}

export class HeapFile {
  table: string;
  pageSize: number = defaultPageSize;
  file: Deno.FsFile | undefined = undefined;
  buf: Uint8Array = new Uint8Array();
  done: boolean = false;
  upperOffset: number = 6;
  lowerOffset: number = defaultPageSize;
  currentPage: number = 1;
  schema: columnDefinition[] | undefined;
  dir: string = defaultTableLocation;

  constructor(
    table: string,
    pageSize: number,
    dir: string = defaultTableLocation
  ) {
    this.table = table;
    this.pageSize = pageSize;
    this.dir = dir;
    this.buf = new Uint8Array(this.pageSize);
  }

  async load() {
    this.schema = await getSchema(this.table, this.dir);
    this.file = await Deno.open(path.join(this.dir, `${this.table}.data`), {
      read: true,
    });

    const numberOfBytesRead = await this.file.read(this.buf);
    if (numberOfBytesRead === null || numberOfBytesRead === 0) {
      this.done = true;
      this.file.close();
    }
    return this;
  }

  read() {
    const offsetBytes = this.buf.slice(this.upperOffset, this.upperOffset + 2);
    const offset = decodeUint16(offsetBytes);
    const rowBytes = this.buf.slice(offset, this.lowerOffset);
    const row = decodeRow(rowBytes, this.schema!);

    if (offset === 0) return null;

    this.upperOffset += 2; // Move to the next offset
    this.lowerOffset = offset;

    return row;
  }

  async insert(rows: rowItem[][]) {
    const tableFileLocation = path.join(this.dir, `${this.table}.data`);
    const fileInfo = await Deno.stat(tableFileLocation);

    this.schema = await getSchema(this.table, this.dir);
    let lastPageNumber = fileInfo.size / this.pageSize;

    this.file = await Deno.open(tableFileLocation, { read: true, write: true });

    if (fileInfo.size !== 0) {
      console.assert(fileInfo.size % this.pageSize === 0);
      await this.file.seek(-this.pageSize, Deno.SeekMode.End);
      await this.file.read(this.buf);
      await this.file.seek(-this.pageSize, Deno.SeekMode.End);

      // deno-lint-ignore no-empty
      while (this.read() !== null) {} // get to next insertion point
    }

    for (const row of rows) {
      const encodedRow = encodeRow(row, this.schema);
      if (this.lowerOffset - this.upperOffset < encodedRow.length + 6) {
        this.file.write(this.buf);

        this.buf = new Uint8Array(this.pageSize);
        this.upperOffset = 6;
        this.lowerOffset = this.pageSize;
        this.buf.set(encodeUint16(lastPageNumber));
        lastPageNumber++;
      }

      const offset = this.lowerOffset - encodedRow.length;
      this.buf.set(encodeUint16(offset), this.upperOffset);
      this.buf.set(encodedRow, offset);

      this.upperOffset += 2;
      this.lowerOffset = offset;
    }
    this.file.write(this.buf);
    this.file.close();
  }

  async nextPage(): Promise<number | null> {
    if (!this.file) {
      console.log("cannot get next page: file does not exist");
      Deno.exit(1);
    }

    const numberOfBytesRead = await this.file.read(this.buf);
    if (numberOfBytesRead === null || numberOfBytesRead === 0) {
      this.done = true;
      this.file.close();
      return null;
    }

    this.upperOffset = 6;
    this.lowerOffset = this.pageSize;
    this.currentPage++;
    return this.currentPage;
  }

  stop() {
    if (this.file) this.file.close();

    this.done = true;
    this.file = undefined;
  }

  async reset() {
    if (this.file) {
      await this.file.seek(0, Deno.SeekMode.Start);

      this.buf = new Uint8Array();
      this.done = false;
      this.upperOffset = 6;
      this.lowerOffset = defaultPageSize;
      this.currentPage = 1;

      const numberOfBytesRead = await this.file.read(this.buf);
      if (numberOfBytesRead === null || numberOfBytesRead === 0) {
        this.done = true;
        this.file.close();
      }
      return;
    }

    console.log("panic: unimplemented");
    Deno.exit(1);
  }
}
