/**
 * Local and Docker entrypoint. Vercel imports server.js and must not listen.
 */
const app = require("./server");

const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log(`Pipeline: http://localhost:${PORT}`);
});

server.on("error", (err) => {
  console.error(err);
  process.exit(1);
});
