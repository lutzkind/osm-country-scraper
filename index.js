const fs = require("fs");
const { createStore } = require("./src/store");
const { createWorker } = require("./src/worker");
const { createApp } = require("./src/server");
const config = require("./src/config");

fs.mkdirSync(config.dataDir, { recursive: true });
fs.mkdirSync(config.exportsDir, { recursive: true });

const store = createStore(config);
const worker = createWorker({ store, config });
const app = createApp({ store, config });

const server = app.listen(config.port, config.host, () => {
  worker
    .start()
    .then(() => {
      console.log(
        `osm-country-scraper listening on http://${config.host}:${config.port}`
      );
    })
    .catch((error) => {
      console.error("Failed to start worker:", error);
      server.close(() => {
        process.exitCode = 1;
      });
    });
});
