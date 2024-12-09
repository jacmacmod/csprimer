import { assertAlmostEquals, assertEquals, assertObjectMatch } from "@std/assert";
import { CSVFileScan, Q, run, encode, decode, encodeString, decodeString, decodeNumber, encodeNumber } from "./main.ts";
import { parseArgs } from "jsr:@std/cli/parse-args";

// read and print each member in proposed binary encoding
Deno.test(async function rtt() {
  const flags = parseArgs(Deno.args, {
    string: ["csv"],
    default: { csv: "../ml-20m/movies.csv" },
  });

  const gen = run(Q([new CSVFileScan(flags.csv)]));
let count = 0;
  for await (const value of gen) {
    count++
    // console.log(count)
    assertEquals(decodeString(encodeString(value.genres as string)), value.genres as string)
    assertEquals(decodeString(encodeString(value.title as string)).length, (value.title as string).length)
    assertEquals(value.movieId as string, String(decodeNumber(encodeNumber(Number(value.movieId)))))
    assertObjectMatch(value, decode(encode(value)));
  }
});
