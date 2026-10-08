import assert from "node:assert/strict";
import test from "node:test";
import { findKeywordMatches } from "./keyword-match.ts";

const page = "Septic Tank Pumping and wastewater treatment. We service pumps.";

test("exact and case-insensitive phrases still match", () => {
  assert.deepEqual(findKeywordMatches(page, ["septic tank"]), ["septic tank"]);
  assert.deepEqual(findKeywordMatches(page, ["M&A"]), []);
  assert.deepEqual(findKeywordMatches("Advisory for M&A deals", ["M&A"]), ["M&A"]);
});

test("stems match plurals and verb forms", () => {
  assert.deepEqual(findKeywordMatches(page, ["pumps"]), ["pumps"]);
  assert.deepEqual(findKeywordMatches(page, ["pumped"]), ["pumped"]);
  assert.deepEqual(findKeywordMatches("Companies servicing tanks", ["company", "services", "tank"]), [
    "company",
    "services",
    "tank",
  ]);
});

test("fuzzy match allows a small typo on longer words", () => {
  assert.deepEqual(findKeywordMatches(page, ["sepic"]), ["sepic"]);
  assert.deepEqual(findKeywordMatches(page, ["cat"]), []);
  assert.deepEqual(findKeywordMatches("the car wash", ["cat"]), []);
});

test("wildcards use * ? and SQL-style % _", () => {
  assert.deepEqual(findKeywordMatches(page, ["pump*", "waste%"]), ["pump*", "waste%"]);
  assert.deepEqual(findKeywordMatches("septic tanks cleaned", ["tank?"]), ["tank?"]);
  assert.deepEqual(findKeywordMatches("septic tank cleaned", ["tank?"]), []);
  assert.deepEqual(findKeywordMatches(page, ["plum*"]), []);
  assert.deepEqual(findKeywordMatches(page, ["*"]), []);
});

test("multi-word keywords match similar neighboring words and joined compounds", () => {
  assert.deepEqual(findKeywordMatches(page, ["septic tanks"]), ["septic tanks"]);
  assert.deepEqual(findKeywordMatches("waste water systems", ["wastewater"]), ["wastewater"]);
  assert.deepEqual(findKeywordMatches(page, ["waste water"]), ["waste water"]);
  assert.deepEqual(findKeywordMatches(page, ["real estate"]), []);
});
