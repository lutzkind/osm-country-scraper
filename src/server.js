const express = require("express");
const { resolveSelectors } = require("./keywords");
const { createJobId } = require("./worker");

function createApp({ store, config }) {
  const app = express();
  app.use(express.json({ limit: "1mb" }));

  app.get("/health", (_req, res) => {
    res.json({ ok: true });
  });

  app.get("/jobs", (_req, res) => {
    res.json({ jobs: store.listJobs() });
  });

  app.post("/jobs", async (req, res, next) => {
    try {
      const country = String(req.body.country || "").trim();
      const keyword = String(req.body.keyword || "").trim();
      const selectors = resolveSelectors(keyword, req.body.selectors);

      if (!country || !keyword) {
        return res.status(400).json({
          error: "country and keyword are required.",
        });
      }

      const id = createJobId();
      store.createJob({ id, country, keyword, selectors });

      res.status(202).json({
        job: store.getJob(id),
        links: buildLinks(req, config, id),
      });
    } catch (error) {
      next(error);
    }
  });

  app.get("/jobs/:jobId", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) {
      return res.status(404).json({ error: "Job not found." });
    }

    res.json({
      job,
      links: buildLinks(req, config, job.id),
    });
  });

  app.get("/jobs/:jobId/leads", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) {
      return res.status(404).json({ error: "Job not found." });
    }

    const limit = Math.min(
      Number.parseInt(req.query.limit, 10) || 100,
      1000
    );
    const offset = Number.parseInt(req.query.offset, 10) || 0;

    res.json({
      jobId: job.id,
      limit,
      offset,
      leads: store.getJobLeads(job.id, { limit, offset }),
    });
  });

  app.post("/jobs/:jobId/cancel", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) {
      return res.status(404).json({ error: "Job not found." });
    }

    store.cancelJob(job.id);
    res.json({ job: store.getJob(job.id) });
  });

  app.get("/jobs/:jobId/download", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) {
      return res.status(404).json({ error: "Job not found." });
    }

    const format = (req.query.format || "csv").toString().toLowerCase();
    const filePath =
      format === "json" ? job.artifactJsonPath : job.artifactCsvPath;

    if (!filePath) {
      return res.status(409).json({
        error: "Artifacts are not ready yet.",
        jobStatus: job.status,
      });
    }

    res.download(filePath);
  });

  app.use((error, _req, res, _next) => {
    const statusCode = error.statusCode || 500;
    res.status(statusCode).json({
      error: error.message || "Unexpected error.",
    });
  });

  return app;
}

function buildLinks(req, config, jobId) {
  const baseUrl =
    config.publicBaseUrl || `${req.protocol}://${req.get("host")}`;

  return {
    self: `${baseUrl}/jobs/${jobId}`,
    leads: `${baseUrl}/jobs/${jobId}/leads`,
    csv: `${baseUrl}/jobs/${jobId}/download?format=csv`,
    json: `${baseUrl}/jobs/${jobId}/download?format=json`,
    cancel: `${baseUrl}/jobs/${jobId}/cancel`,
  };
}

module.exports = {
  createApp,
};
