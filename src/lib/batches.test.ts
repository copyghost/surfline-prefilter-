import assert from "node:assert/strict";
import test from "node:test";
import { batchRanges } from "./batches.ts";

test("covers every row past the first batch of 25", () => {
  assert.deepEqual(batchRanges(40, 25), [
    { start: 0, end: 25 },
    { start: 25, end: 40 },
  ]);
  assert.deepEqual(batchRanges(25, 25), [{ start: 0, end: 25 }]);
  assert.deepEqual(batchRanges(0, 25), []);
});
