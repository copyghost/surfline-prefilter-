import assert from "node:assert/strict";
import test from "node:test";
import { closeLocalChrome, findChromeExecutable, renderWithLocalChrome } from "./local-chrome.ts";

test("uses an explicit Chrome path when that file exists", () => {
  const found = findChromeExecutable({ CHROME_PATH: "/tmp/chrome" }, (path) => path === "/tmp/chrome");
  assert.equal(found, "/tmp/chrome");
});

test("returns null when Chrome is not installed", () => {
  assert.equal(findChromeExecutable({}, () => false), null);
});

test("local Chrome renders a public page", { skip: findChromeExecutable() ? false : "Chrome is not installed" }, async () => {
  const html = await renderWithLocalChrome("https://example.com", 20000);
  assert.match(html, /Example Domain/);
  await closeLocalChrome();
});
