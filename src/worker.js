const crypto = require("crypto");
const {
  bboxAreaDegrees,
  bboxIntersectsGeometry,
  buildSeedBBoxes,
  splitBBox,
  canSplitBBox,
} = require("./geo");
const { resolveCountry, queryOverpass } = require("./osm");
const { writeArtifacts } = require("./exporters");

function createWorker({ store, config, nocoDb = null }) {
  let timer = null;
  let busy = false;

  return {
    async start() {
      await bootstrapPendingJobs();
      timer = setInterval(() => {
        this.tick().catch((error) => {
          console.error("Worker tick failed:", error);
        });
      }, config.workerPollMs);
      timer.unref?.();
    },

    stop() {
      if (timer) {
        clearInterval(timer);
      }
    },

    async tick() {
      if (busy) {
        return;
      }

      busy = true;
      try {
        await bootstrapPendingJobs();
        const shard = store.claimNextShard();
        if (!shard) {
          await maybeSyncRunningJobs();
          return;
        }

        const job = store.getJob(shard.jobId);
        if (!job || job.status !== "running") {
          return;
        }

        const geometry = job.countryGeometry
          ? {
              type: "Feature",
              geometry: job.countryGeometry,
            }
          : null;

        await processShard(job, shard, geometry);
        await maybeSyncRunningJobs();
        await maybeFinalizeJob(job.id);
      } finally {
        busy = false;
      }
    },
  };

  async function bootstrapPendingJobs() {
    const jobs = store.listJobs().filter((job) => job.status === "pending");

    for (const job of jobs) {
      try {
        if (job.totalShards > 0 || job.startedAt) {
          store.resumeJob(job.id);
          continue;
        }

        const countryData = await resolveCountry(job.country, config);
        const geometry = countryData.geometry || null;
        const initialShards = buildSeedBBoxes(countryData.bbox, geometry, config);
        store.seedJob(
          job.id,
          countryData,
          initialShards.length > 0 ? initialShards : [countryData.bbox]
        );
      } catch (error) {
        store.failJob(job.id, error.message);
      }
    }
  }

  async function processShard(job, shard, geometry) {
    if (geometry?.geometry && !bboxIntersectsGeometry(shard.bbox, geometry)) {
      store.skipShard(shard.id, "Shard does not intersect the country geometry.");
      return;
    }

    const canSplit =
      shard.depth < config.maxShardDepth && canSplitBBox(shard.bbox, config);
    const isOversized =
      bboxAreaDegrees(shard.bbox) > config.preQuerySplitAreaDegSq;

    if (canSplit && isOversized) {
      store.splitShard(shard.id, splitBBox(shard.bbox));
      return;
    }

    try {
      const response = await queryOverpass({
        bbox: shard.bbox,
        selectors: job.selectors,
        geometry,
        config,
      });

      if (store.getJob(job.id)?.status === "canceled") {
        return;
      }

      if (
        response.rawCount >= config.resultSplitThreshold &&
        shard.depth < config.maxShardDepth &&
        canSplitBBox(shard.bbox, config)
      ) {
        store.splitShard(shard.id, splitBBox(shard.bbox));
        return;
      }

      store.completeShard(shard.id, response.leads);
    } catch (error) {
      const isRateOrTimeout =
        error.name === "AbortError" ||
        error.statusCode === 429 ||
        error.statusCode === 504 ||
        /timeout/i.test(error.message);

      if (store.getJob(job.id)?.status === "canceled") {
        return;
      }

      if (isRateOrTimeout && canSplit && shard.depth <= config.immediateSplitDepth) {
        store.splitShard(shard.id, splitBBox(shard.bbox));
        return;
      }

      if (shard.attemptCount < config.retryLimit) {
        const delay = config.retryBaseDelayMs * 2 ** (shard.attemptCount - 1);
        store.retryShard(shard.id, error.message, delay);
        return;
      }

      if (canSplit) {
        store.splitShard(shard.id, splitBBox(shard.bbox));
        return;
      }

      store.failShard(shard.id, error.message);
    }
  }

  async function maybeFinalizeJob(jobId) {
    const unfinished = store.refreshJobStats(jobId);
    if (unfinished > 0) {
      return;
    }

    const job = store.getJob(jobId);
    if (!job || ["completed", "partial", "failed", "canceled"].includes(job.status)) {
      return;
    }

    if (job.leadCount === 0 && job.failedShards === job.totalShards) {
      store.finalizeJob(jobId, "failed", "All shards failed.");
      return;
    }

    const artifacts = writeArtifacts(store, config, jobId);
    const status = job.failedShards > 0 ? "partial" : "completed";
    const message =
      status === "completed"
        ? "Completed successfully."
        : "Completed with failed shards.";
    store.finalizeJob(jobId, status, message, artifacts);

    if (nocoDb) {
      await nocoDb.syncCompletedJobIfEnabled(jobId);
    }
  }

  async function maybeSyncRunningJobs() {
    if (!nocoDb?.getRunningJobSyncIdsDue) {
      return;
    }

    const dueJobIds = nocoDb.getRunningJobSyncIdsDue();
    for (const jobId of dueJobIds) {
      try {
        await nocoDb.syncJob(jobId);
      } catch (error) {
        console.error(`Incremental NocoDB sync failed for job ${jobId}:`, error.message);
      }
    }
  }
}

function createJobId() {
  return crypto.randomUUID();
}

module.exports = {
  createWorker,
  createJobId,
};
