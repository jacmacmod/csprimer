import {
  columnDefinition,
  decodeRow,
  encodeRow,
  getSchema,
  decodeUint16,
  encodeUint16,
} from "./util.ts";

import { row } from "./type.ts";

export const defaultPageSize = 1024;

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
  lowerIdx: number = 6;
  prevOffset: number = defaultPageSize;
  currentPage: number = 1;
  schema: columnDefinition[] | undefined;

  constructor(table: string, pageSize: number) {
    this.table = table;
    this.pageSize = pageSize;
  }

  async load() {
    this.schema = await getSchema(this.table);
    this.file = await Deno.open(`tables/${this.table}.data`, { read: true });
    this.buf = new Uint8Array(this.pageSize);

    const numberOfBytesRead = await this.file.read(this.buf);
    if (numberOfBytesRead === null || numberOfBytesRead === 0) {
      this.done = true;
      this.file.close();
    }
    return this;
  }

  read() {
    const offsetBytes = this.buf.slice(this.lowerIdx, this.lowerIdx + 2);
    const offset = decodeUint16(offsetBytes);
    const rowBytes = this.buf.slice(offset, this.prevOffset);
    const row = decodeRow(rowBytes, this.schema!);

    if (offset === 0) return null;

    this.lowerIdx += 2; // Move to the next offset
    this.prevOffset = offset;

    return row;
  }

  async insert(rows: row[]) {
    const fileInfo = await Deno.stat(`${this.table}.data`);
    this.schema = await getSchema(this.table);
    console.assert(fileInfo.size % this.pageSize !== 0);
    let lastPageNumber = fileInfo.size / this.pageSize;

    this.file = await Deno.open(`tables/${this.table}.data`, {
      read: true,
      write: true,
    });

    await this.file.seek(-this.pageSize, Deno.SeekMode.End);
    this.buf = new Uint8Array(this.pageSize);
    await this.file.read(this.buf);
    await this.file.seek(-this.pageSize, Deno.SeekMode.End);

    while (this.read() !== null) {}

    for (const row of rows) {
      const encodedRow = encodeRow(row, this.schema);
      if (this.prevOffset - this.lowerIdx < encodedRow.length + 6) {
        this.file.write(this.buf);

        this.buf = new Uint8Array(this.pageSize);
        this.lowerIdx = 6;
        this.prevOffset = this.pageSize;
        this.buf.set(encodeUint16(lastPageNumber));
        lastPageNumber++;
      }

      const offset = this.prevOffset - encodedRow.length;
      this.buf.set(encodeUint16(offset), this.lowerIdx);
      this.buf.set(encodedRow, offset);

      this.lowerIdx += 2;
      this.prevOffset = offset;
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

    this.lowerIdx = 6;
    this.prevOffset = this.pageSize;
    this.currentPage++;
    return this.currentPage;
  }

  stop() {
    if (this.file) this.file.close();

    this.done = true;
    this.file = undefined;
  }
}
