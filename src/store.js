const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");

function nowIso() {
  return new Date().toISOString();
}

function createStore(config) {
  fs.mkdirSync(path.dirname(config.dbPath), { recursive: true });
  fs.mkdirSync(config.exportsDir, { recursive: true });

  const db = new Database(config.dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");

  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      country TEXT NOT NULL,
      keyword TEXT NOT NULL,
      selectors_json TEXT NOT NULL,
      status TEXT NOT NULL,
      message TEXT,
      country_name TEXT,
      country_code TEXT,
      country_bbox_json TEXT,
      country_geometry_json TEXT,
      total_shards INTEGER NOT NULL DEFAULT 0,
      completed_shards INTEGER NOT NULL DEFAULT 0,
      failed_shards INTEGER NOT NULL DEFAULT 0,
      lead_count INTEGER NOT NULL DEFAULT 0,
      artifact_csv_path TEXT,
      artifact_json_path TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      started_at TEXT,
      finished_at TEXT
    );

    CREATE TABLE IF NOT EXISTS shards (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id TEXT NOT NULL,
      bbox_json TEXT NOT NULL,
      depth INTEGER NOT NULL,
      status TEXT NOT NULL,
      result_count INTEGER NOT NULL DEFAULT 0,
      attempt_count INTEGER NOT NULL DEFAULT 0,
      next_run_at TEXT NOT NULL,
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_shards_status_next_run
      ON shards(status, next_run_at);

    CREATE TABLE IF NOT EXISTS leads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id TEXT NOT NULL,
      osm_type TEXT NOT NULL,
      osm_id TEXT NOT NULL,
      name TEXT,
      category TEXT,
      subcategory TEXT,
      website TEXT,
      phone TEXT,
      email TEXT,
      address TEXT,
      lat REAL NOT NULL,
      lon REAL NOT NULL,
      source_bbox_json TEXT NOT NULL,
      tags_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(job_id, osm_type, osm_id),
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      last_seen_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_sessions_expires_at
      ON sessions(expires_at);

    CREATE TABLE IF NOT EXISTS app_settings (
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS nocodb_sync_state (
      job_id TEXT PRIMARY KEY,
      last_synced_lead_id INTEGER NOT NULL DEFAULT 0,
      last_synced_at TEXT,
      last_status TEXT NOT NULL DEFAULT 'idle',
      last_message TEXT,
      last_started_at TEXT,
      last_finished_at TEXT,
      synced_record_count INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );
  `);

  resetRunningShards(db);
  cleanupExpiredSessions(db);

  return {
    db,
    createJob(input) {
      const timestamp = nowIso();
      db.prepare(
        `
          INSERT INTO jobs (
            id, country, keyword, selectors_json, status, message,
            country_name, country_code, country_bbox_json, country_geometry_json,
            created_at, updated_at
          ) VALUES (
            @id, @country, @keyword, @selectorsJson, 'pending', 'Queued',
            NULL, NULL, NULL, NULL, @timestamp, @timestamp
          )
        `
      ).run({
        id: input.id,
        country: input.country,
        keyword: input.keyword,
        selectorsJson: JSON.stringify(input.selectors),
        timestamp,
      });
    },

    seedJob(jobId, countryData) {
      const timestamp = nowIso();
      db.transaction(() => {
        db.prepare(
          `
            UPDATE jobs
            SET status = 'running',
                message = 'Running',
                country_name = @countryName,
                country_code = @countryCode,
                country_bbox_json = @bboxJson,
                country_geometry_json = @geometryJson,
                started_at = COALESCE(started_at, @timestamp),
                updated_at = @timestamp
            WHERE id = @jobId
          `
        ).run({
          jobId,
          countryName: countryData.displayName,
          countryCode: countryData.countryCode,
          bboxJson: JSON.stringify(countryData.bbox),
          geometryJson: JSON.stringify(countryData.geometry?.geometry || null),
          timestamp,
        });

        db.prepare(
          `
            INSERT INTO shards (
              job_id, bbox_json, depth, status, next_run_at,
              created_at, updated_at
            ) VALUES (
              @jobId, @bboxJson, 0, 'pending', @timestamp, @timestamp, @timestamp
            )
          `
        ).run({
          jobId,
          bboxJson: JSON.stringify(countryData.bbox),
          timestamp,
        });
      })();

      this.refreshJobStats(jobId);
    },

    failJob(jobId, errorMessage) {
      db.prepare(
        `
          UPDATE jobs
          SET status = 'failed',
              message = @errorMessage,
              finished_at = @timestamp,
              updated_at = @timestamp
          WHERE id = @jobId
        `
      ).run({ jobId, errorMessage, timestamp: nowIso() });
    },

    listJobs() {
      return db
        .prepare(
          `
            SELECT *
            FROM jobs
            ORDER BY created_at DESC
          `
        )
        .all()
        .map(deserializeJobRow);
    },

    getJob(jobId) {
      const row = db
        .prepare(
          `
            SELECT *
            FROM jobs
            WHERE id = ?
          `
        )
        .get(jobId);

      return row ? deserializeJobRow(row) : null;
    },

    getJobLeads(jobId, { limit = 100, offset = 0 } = {}) {
      return db
        .prepare(
          `
            SELECT *
            FROM leads
            WHERE job_id = ?
            ORDER BY id ASC
            LIMIT ?
            OFFSET ?
          `
        )
        .all(jobId, limit, offset)
        .map(deserializeLeadRow);
    },

    countJobShards(jobId, status = null) {
      const row = status
        ? db
            .prepare(
              `
                SELECT COUNT(*) AS total
                FROM shards
                WHERE job_id = ?
                  AND status = ?
              `
            )
            .get(jobId, status)
        : db
            .prepare(
              `
                SELECT COUNT(*) AS total
                FROM shards
                WHERE job_id = ?
              `
            )
            .get(jobId);

      return row?.total || 0;
    },

    listJobShards(jobId, { status = null, limit = 100, offset = 0 } = {}) {
      const rows = status
        ? db
            .prepare(
              `
                SELECT *
                FROM shards
                WHERE job_id = ?
                  AND status = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                OFFSET ?
              `
            )
            .all(jobId, status, limit, offset)
        : db
            .prepare(
              `
                SELECT *
                FROM shards
                WHERE job_id = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                OFFSET ?
              `
            )
            .all(jobId, limit, offset);

      return rows.map(deserializeShardRow);
    },

    getJobErrors(jobId, { limit = 25 } = {}) {
      return db
        .prepare(
          `
            SELECT *
            FROM shards
            WHERE job_id = ?
              AND COALESCE(last_error, '') != ''
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
          `
        )
        .all(jobId, limit)
        .map(deserializeShardRow);
    },

    getJobStats(jobId) {
      const job = this.getJob(jobId);
      if (!job) {
        return null;
      }

      const shardStats = db
        .prepare(
          `
            SELECT
              COUNT(*) AS total_shards,
              SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_shards,
              SUM(CASE WHEN status = 'retry' THEN 1 ELSE 0 END) AS retry_shards,
              SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_shards,
              SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done_shards,
              SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_shards,
              SUM(CASE WHEN status = 'split' THEN 1 ELSE 0 END) AS split_shards,
              SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped_shards,
              SUM(CASE WHEN status = 'canceled' THEN 1 ELSE 0 END) AS canceled_shards,
              SUM(CASE WHEN status IN ('done', 'failed', 'split', 'skipped', 'canceled') THEN 1 ELSE 0 END) AS terminal_shards,
              SUM(result_count) AS shard_result_count,
              SUM(attempt_count) AS total_attempts,
              MAX(depth) AS max_depth,
              MIN(CASE WHEN status IN ('pending', 'retry') THEN next_run_at END) AS next_run_at,
              MAX(updated_at) AS last_activity_at
            FROM shards
            WHERE job_id = ?
          `
        )
        .get(jobId);

      const websiteStats = db
        .prepare(
          `
            SELECT
              SUM(CASE WHEN COALESCE(website, '') != '' THEN 1 ELSE 0 END) AS leads_with_website,
              SUM(CASE WHEN COALESCE(email, '') != '' THEN 1 ELSE 0 END) AS leads_with_email,
              SUM(CASE WHEN COALESCE(phone, '') != '' THEN 1 ELSE 0 END) AS leads_with_phone
            FROM leads
            WHERE job_id = ?
          `
        )
        .get(jobId);

      const recentLeadStats = db
        .prepare(
          `
            SELECT
              SUM(CASE WHEN created_at >= @oneHourAgo THEN 1 ELSE 0 END) AS leads_last_hour,
              SUM(CASE WHEN created_at >= @oneDayAgo THEN 1 ELSE 0 END) AS leads_last_day
            FROM leads
            WHERE job_id = @jobId
          `
        )
        .get({
          jobId,
          oneHourAgo: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
          oneDayAgo: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
        });

      const referenceStart = job.startedAt || job.createdAt;
      const referenceEnd =
        job.finishedAt && ["completed", "partial", "failed", "canceled"].includes(job.status)
          ? job.finishedAt
          : nowIso();
      const elapsedMs = Math.max(
        0,
        new Date(referenceEnd).getTime() - new Date(referenceStart).getTime()
      );
      const elapsedHours = elapsedMs / (60 * 60 * 1000);
      const safeElapsedHours = elapsedHours > 0 ? elapsedHours : null;

      return {
        statusCounts: {
          pending: shardStats.pending_shards || 0,
          retry: shardStats.retry_shards || 0,
          running: shardStats.running_shards || 0,
          done: shardStats.done_shards || 0,
          failed: shardStats.failed_shards || 0,
          split: shardStats.split_shards || 0,
          skipped: shardStats.skipped_shards || 0,
          canceled: shardStats.canceled_shards || 0,
          terminal: shardStats.terminal_shards || 0,
          total: shardStats.total_shards || 0,
        },
        leadCoverage: {
          leadsWithWebsite: websiteStats.leads_with_website || 0,
          leadsWithEmail: websiteStats.leads_with_email || 0,
          leadsWithPhone: websiteStats.leads_with_phone || 0,
        },
        recentActivity: {
          leadsLastHour: recentLeadStats.leads_last_hour || 0,
          leadsLastDay: recentLeadStats.leads_last_day || 0,
          nextRunAt: shardStats.next_run_at || null,
          lastActivityAt: shardStats.last_activity_at || job.updatedAt,
        },
        throughput: {
          leadsPerHour: safeElapsedHours
            ? Number((job.leadCount / safeElapsedHours).toFixed(2))
            : null,
          completedShardsPerHour: safeElapsedHours
            ? Number((job.completedShards / safeElapsedHours).toFixed(2))
            : null,
        },
        progress: {
          knownShardCompletionRatio:
            (shardStats.total_shards || 0) > 0
              ? Number(
                  (
                    ((shardStats.terminal_shards || 0) / shardStats.total_shards) *
                    100
                  ).toFixed(2)
                )
              : 0,
        },
        depth: {
          maxDepth: shardStats.max_depth || 0,
        },
        attempts: {
          totalAttempts: shardStats.total_attempts || 0,
        },
        elapsed: {
          startedAt: referenceStart,
          finishedAt: job.finishedAt,
          elapsedMs,
          elapsedHours: Number(elapsedHours.toFixed(2)),
        },
      };
    },

    createSession({ id, username, expiresAt }) {
      const timestamp = nowIso();
      db.prepare(
        `
          INSERT INTO sessions (
            id, username, expires_at, created_at, last_seen_at
          ) VALUES (
            @id, @username, @expiresAt, @timestamp, @timestamp
          )
        `
      ).run({
        id,
        username,
        expiresAt,
        timestamp,
      });
    },

    getSession(sessionId) {
      const row = db
        .prepare(
          `
            SELECT *
            FROM sessions
            WHERE id = ?
          `
        )
        .get(sessionId);

      if (!row) {
        return null;
      }

      if (new Date(row.expires_at).getTime() <= Date.now()) {
        db.prepare(`DELETE FROM sessions WHERE id = ?`).run(sessionId);
        return null;
      }

      return {
        id: row.id,
        username: row.username,
        expiresAt: row.expires_at,
        createdAt: row.created_at,
        lastSeenAt: row.last_seen_at,
      };
    },

    touchSession(sessionId, expiresAt) {
      const timestamp = nowIso();
      db.prepare(
        `
          UPDATE sessions
          SET expires_at = @expiresAt,
              last_seen_at = @timestamp
          WHERE id = @id
        `
      ).run({
        id: sessionId,
        expiresAt,
        timestamp,
      });
    },

    deleteSession(sessionId) {
      db.prepare(`DELETE FROM sessions WHERE id = ?`).run(sessionId);
    },

    cleanupExpiredSessions() {
      cleanupExpiredSessions(db);
    },

    getAppSetting(key, fallback = null) {
      const row = db
        .prepare(
          `
            SELECT value_json
            FROM app_settings
            WHERE key = ?
          `
        )
        .get(key);

      if (!row) {
        return fallback;
      }

      return parseJsonOrFallback(row.value_json, fallback);
    },

    setAppSettings(settings) {
      const timestamp = nowIso();
      const upsert = db.prepare(
        `
          INSERT INTO app_settings (key, value_json, updated_at)
          VALUES (@key, @valueJson, @timestamp)
          ON CONFLICT(key) DO UPDATE SET
            value_json = excluded.value_json,
            updated_at = excluded.updated_at
        `
      );

      db.transaction(() => {
        for (const [key, value] of Object.entries(settings)) {
          upsert.run({
            key,
            valueJson: JSON.stringify(value),
            timestamp,
          });
        }
      })();
    },

    getNocoDbConfig() {
      return sanitizeNocoDbConfig({
        baseUrl: this.getAppSetting("nocodb.baseUrl", config.nocoDb.baseUrl),
        apiToken: this.getAppSetting("nocodb.apiToken", config.nocoDb.apiToken),
        baseId: this.getAppSetting("nocodb.baseId", config.nocoDb.baseId),
        tableId: this.getAppSetting("nocodb.tableId", config.nocoDb.tableId),
        autoSyncOnCompletion: this.getAppSetting(
          "nocodb.autoSyncOnCompletion",
          config.nocoDb.autoSyncOnCompletion
        ),
        autoSyncIntervalMinutes: this.getAppSetting(
          "nocodb.autoSyncIntervalMinutes",
          config.nocoDb.autoSyncIntervalMinutes
        ),
        autoCreateColumns: this.getAppSetting(
          "nocodb.autoCreateColumns",
          config.nocoDb.autoCreateColumns
        ),
      });
    },

    saveNocoDbConfig(input) {
      const current = this.getNocoDbConfig();
      const next = sanitizeNocoDbConfig({
        ...current,
        ...input,
        apiToken:
          input.apiToken == null || input.apiToken === ""
            ? current.apiToken
            : input.apiToken,
      });

      this.setAppSettings({
        "nocodb.baseUrl": next.baseUrl,
        "nocodb.apiToken": next.apiToken,
        "nocodb.baseId": next.baseId,
        "nocodb.tableId": next.tableId,
        "nocodb.autoSyncOnCompletion": next.autoSyncOnCompletion,
        "nocodb.autoSyncIntervalMinutes": next.autoSyncIntervalMinutes,
        "nocodb.autoCreateColumns": next.autoCreateColumns,
      });

      return next;
    },

    getNocoDbSyncState(jobId) {
      const row = db
        .prepare(
          `
            SELECT *
            FROM nocodb_sync_state
            WHERE job_id = ?
          `
        )
        .get(jobId);

      return row ? deserializeSyncStateRow(row) : defaultSyncState(jobId);
    },

    markNocoDbSyncStarted(jobId) {
      const timestamp = nowIso();
      db.prepare(
        `
          INSERT INTO nocodb_sync_state (
            job_id, last_status, last_message, last_started_at, last_finished_at
          ) VALUES (
            @jobId, 'running', 'Sync in progress.', @timestamp, NULL
          )
          ON CONFLICT(job_id) DO UPDATE SET
            last_status = 'running',
            last_message = 'Sync in progress.',
            last_started_at = excluded.last_started_at,
            last_finished_at = NULL
        `
      ).run({ jobId, timestamp });
    },

    markNocoDbSyncSuccess(jobId, input) {
      const timestamp = nowIso();
      db.prepare(
        `
          INSERT INTO nocodb_sync_state (
            job_id, last_synced_lead_id, last_synced_at, last_status,
            last_message, last_started_at, last_finished_at, synced_record_count
          ) VALUES (
            @jobId, @lastSyncedLeadId, @timestamp, 'success',
            @message, COALESCE(@startedAt, @timestamp), @timestamp, @syncedRecordCount
          )
          ON CONFLICT(job_id) DO UPDATE SET
            last_synced_lead_id = @lastSyncedLeadId,
            last_synced_at = @timestamp,
            last_status = 'success',
            last_message = @message,
            last_finished_at = @timestamp,
            synced_record_count = COALESCE(nocodb_sync_state.synced_record_count, 0) + @syncedRecordCount
        `
      ).run({
        jobId,
        lastSyncedLeadId: input.lastSyncedLeadId || 0,
        syncedRecordCount: input.syncedRecordCount || 0,
        message: input.message || "Sync completed.",
        startedAt: input.startedAt || null,
        timestamp,
      });
    },

    markNocoDbSyncFailure(jobId, message) {
      const timestamp = nowIso();
      db.prepare(
        `
          INSERT INTO nocodb_sync_state (
            job_id, last_status, last_message, last_started_at, last_finished_at
          ) VALUES (
            @jobId, 'failed', @message, @timestamp, @timestamp
          )
          ON CONFLICT(job_id) DO UPDATE SET
            last_status = 'failed',
            last_message = @message,
            last_finished_at = @timestamp
        `
      ).run({ jobId, message, timestamp });
    },

    cancelJob(jobId) {
      const timestamp = nowIso();
      db.transaction(() => {
        db.prepare(
          `
            UPDATE jobs
            SET status = 'canceled',
                message = 'Canceled',
                finished_at = @timestamp,
                updated_at = @timestamp
            WHERE id = @jobId
          `
        ).run({ jobId, timestamp });

        db.prepare(
          `
            UPDATE shards
            SET status = 'canceled',
                updated_at = @timestamp
            WHERE job_id = @jobId
              AND status IN ('pending', 'retry', 'running')
          `
        ).run({ jobId, timestamp });
      })();

      this.refreshJobStats(jobId);
      return this.getJob(jobId);
    },

    deleteJob(jobId) {
      const job = this.getJob(jobId);
      if (!job) {
        return null;
      }

      if (!["completed", "partial", "failed", "canceled"].includes(job.status)) {
        const error = new Error(
          "Only completed, partial, failed, or canceled jobs can be deleted."
        );
        error.statusCode = 409;
        throw error;
      }

      const artifactPaths = [job.artifactCsvPath, job.artifactJsonPath].filter(Boolean);

      db.transaction(() => {
        db.prepare(`DELETE FROM jobs WHERE id = ?`).run(jobId);
      })();

      for (const artifactPath of artifactPaths) {
        try {
          fs.unlinkSync(artifactPath);
        } catch (error) {
          if (error.code !== "ENOENT") {
            throw error;
          }
        }
      }

      return job;
    },

    getJobLeadsAfterId(jobId, leadId = 0, { limit = 100 } = {}) {
      return db
        .prepare(
          `
            SELECT *
            FROM leads
            WHERE job_id = ?
              AND id > ?
            ORDER BY id ASC
            LIMIT ?
          `
        )
        .all(jobId, leadId, limit)
        .map(deserializeLeadRow);
    },

    claimNextShard() {
      const timestamp = nowIso();
      const row = db
        .prepare(
          `
            SELECT s.*
            FROM shards s
            JOIN jobs j ON j.id = s.job_id
            WHERE s.status IN ('pending', 'retry')
              AND s.next_run_at <= @timestamp
              AND j.status = 'running'
            ORDER BY s.updated_at ASC
            LIMIT 1
          `
        )
        .get({ timestamp });

      if (!row) {
        return null;
      }

      db.prepare(
        `
          UPDATE shards
          SET status = 'running',
              attempt_count = attempt_count + 1,
              updated_at = @timestamp
          WHERE id = @id
        `
      ).run({ id: row.id, timestamp });

      return this.getShard(row.id);
    },

    getShard(shardId) {
      const row = db
        .prepare(
          `
            SELECT *
            FROM shards
            WHERE id = ?
          `
        )
        .get(shardId);

      return row ? deserializeShardRow(row) : null;
    },

    splitShard(shardId, childBBoxes) {
      const shard = this.getShard(shardId);
      const timestamp = nowIso();

      db.transaction(() => {
        db.prepare(
          `
            UPDATE shards
            SET status = 'split',
                updated_at = @timestamp
            WHERE id = @id
          `
        ).run({ id: shardId, timestamp });

        const insert = db.prepare(
          `
            INSERT INTO shards (
              job_id, bbox_json, depth, status, next_run_at, created_at, updated_at
            ) VALUES (
              @jobId, @bboxJson, @depth, 'pending', @timestamp, @timestamp, @timestamp
            )
          `
        );

        for (const bbox of childBBoxes) {
          insert.run({
            jobId: shard.jobId,
            bboxJson: JSON.stringify(bbox),
            depth: shard.depth + 1,
            timestamp,
          });
        }
      })();

      this.refreshJobStats(shard.jobId);
      return shard.jobId;
    },

    skipShard(shardId, message) {
      const shard = this.getShard(shardId);
      db.prepare(
        `
          UPDATE shards
          SET status = 'skipped',
              last_error = @message,
              updated_at = @timestamp
          WHERE id = @id
        `
      ).run({ id: shardId, message, timestamp: nowIso() });
      this.refreshJobStats(shard.jobId);
      return shard.jobId;
    },

    completeShard(shardId, leads) {
      const shard = this.getShard(shardId);
      const timestamp = nowIso();

      db.transaction(() => {
        const insert = db.prepare(
          `
            INSERT INTO leads (
              job_id, osm_type, osm_id, name, category, subcategory, website,
              phone, email, address, lat, lon, source_bbox_json, tags_json,
              created_at, updated_at
            ) VALUES (
              @jobId, @osmType, @osmId, @name, @category, @subcategory, @website,
              @phone, @email, @address, @lat, @lon, @sourceBBoxJson, @tagsJson,
              @timestamp, @timestamp
            )
            ON CONFLICT(job_id, osm_type, osm_id) DO UPDATE SET
              name = excluded.name,
              category = excluded.category,
              subcategory = excluded.subcategory,
              website = CASE
                WHEN COALESCE(leads.website, '') = '' THEN excluded.website
                ELSE leads.website
              END,
              phone = CASE
                WHEN COALESCE(leads.phone, '') = '' THEN excluded.phone
                ELSE leads.phone
              END,
              email = CASE
                WHEN COALESCE(leads.email, '') = '' THEN excluded.email
                ELSE leads.email
              END,
              address = CASE
                WHEN COALESCE(leads.address, '') = '' THEN excluded.address
                ELSE leads.address
              END,
              tags_json = excluded.tags_json,
              updated_at = excluded.updated_at
          `
        );

        for (const lead of leads) {
          insert.run({
            jobId: shard.jobId,
            osmType: lead.osmType,
            osmId: lead.osmId,
            name: lead.name,
            category: lead.category,
            subcategory: lead.subcategory,
            website: lead.website,
            phone: lead.phone,
            email: lead.email,
            address: lead.address,
            lat: lead.lat,
            lon: lead.lon,
            sourceBBoxJson: JSON.stringify(lead.bbox),
            tagsJson: JSON.stringify(lead.tags),
            timestamp,
          });
        }

        db.prepare(
          `
            UPDATE shards
            SET status = 'done',
                result_count = @resultCount,
                last_error = NULL,
                updated_at = @timestamp
            WHERE id = @id
          `
        ).run({
          id: shardId,
          resultCount: leads.length,
          timestamp,
        });
      })();

      this.refreshJobStats(shard.jobId);
      return shard.jobId;
    },

    retryShard(shardId, errorMessage, delayMs) {
      const shard = this.getShard(shardId);
      const nextRunAt = new Date(Date.now() + delayMs).toISOString();
      db.prepare(
        `
          UPDATE shards
          SET status = 'retry',
              last_error = @errorMessage,
              next_run_at = @nextRunAt,
              updated_at = @timestamp
          WHERE id = @id
        `
      ).run({
        id: shardId,
        errorMessage,
        nextRunAt,
        timestamp: nowIso(),
      });
      this.refreshJobStats(shard.jobId);
      return shard.jobId;
    },

    failShard(shardId, errorMessage) {
      const shard = this.getShard(shardId);
      db.prepare(
        `
          UPDATE shards
          SET status = 'failed',
              last_error = @errorMessage,
              updated_at = @timestamp
          WHERE id = @id
        `
      ).run({ id: shardId, errorMessage, timestamp: nowIso() });
      this.refreshJobStats(shard.jobId);
      return shard.jobId;
    },

    refreshJobStats(jobId) {
      const shardStats = db
        .prepare(
          `
            SELECT
              COUNT(*) AS total_shards,
              SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS completed_shards,
              SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_shards,
              SUM(CASE WHEN status IN ('pending', 'retry', 'running') THEN 1 ELSE 0 END) AS unfinished_shards
            FROM shards
            WHERE job_id = ?
          `
        )
        .get(jobId);

      const leadStats = db
        .prepare(
          `
            SELECT COUNT(*) AS lead_count
            FROM leads
            WHERE job_id = ?
          `
        )
        .get(jobId);

      db.prepare(
        `
          UPDATE jobs
          SET total_shards = @totalShards,
              completed_shards = @completedShards,
              failed_shards = @failedShards,
              lead_count = @leadCount,
              updated_at = @timestamp
          WHERE id = @jobId
        `
      ).run({
        jobId,
        totalShards: shardStats.total_shards || 0,
        completedShards: shardStats.completed_shards || 0,
        failedShards: shardStats.failed_shards || 0,
        leadCount: leadStats.lead_count || 0,
        timestamp: nowIso(),
      });

      return shardStats.unfinished_shards || 0;
    },

    finalizeJob(jobId, status, message, artifacts = {}) {
      db.prepare(
        `
          UPDATE jobs
          SET status = @status,
              message = @message,
              artifact_csv_path = COALESCE(@csvPath, artifact_csv_path),
              artifact_json_path = COALESCE(@jsonPath, artifact_json_path),
              finished_at = @timestamp,
              updated_at = @timestamp
          WHERE id = @jobId
        `
      ).run({
        jobId,
        status,
        message,
        csvPath: artifacts.csvPath || null,
        jsonPath: artifacts.jsonPath || null,
        timestamp: nowIso(),
      });
    },
  };
}

function resetRunningShards(db) {
  const timestamp = nowIso();

  db.prepare(
    `
      UPDATE shards
      SET status = 'retry',
          next_run_at = @timestamp,
          updated_at = @timestamp,
          last_error = COALESCE(last_error, 'Recovered after process restart.')
      WHERE status = 'running'
    `
  ).run({ timestamp });

  db.prepare(
    `
      UPDATE jobs
      SET status = CASE
        WHEN status = 'running' THEN 'pending'
        ELSE status
      END,
      message = CASE
        WHEN status = 'running' THEN 'Recovered after process restart.'
        ELSE message
      END,
      updated_at = @timestamp
      WHERE status = 'running'
    `
  ).run({ timestamp });
}

function cleanupExpiredSessions(db) {
  db.prepare(
    `
      DELETE FROM sessions
      WHERE expires_at <= @timestamp
    `
  ).run({ timestamp: nowIso() });
}

function deserializeJobRow(row) {
  return {
    id: row.id,
    country: row.country,
    keyword: row.keyword,
    selectors: JSON.parse(row.selectors_json),
    status: row.status,
    message: row.message,
    countryName: row.country_name,
    countryCode: row.country_code,
    countryBBox: row.country_bbox_json ? JSON.parse(row.country_bbox_json) : null,
    countryGeometry: row.country_geometry_json
      ? JSON.parse(row.country_geometry_json)
      : null,
    totalShards: row.total_shards,
    completedShards: row.completed_shards,
    failedShards: row.failed_shards,
    leadCount: row.lead_count,
    artifactCsvPath: row.artifact_csv_path,
    artifactJsonPath: row.artifact_json_path,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
  };
}

function deserializeShardRow(row) {
  return {
    id: row.id,
    jobId: row.job_id,
    bbox: JSON.parse(row.bbox_json),
    depth: row.depth,
    status: row.status,
    resultCount: row.result_count,
    attemptCount: row.attempt_count,
    nextRunAt: row.next_run_at,
    lastError: row.last_error,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function deserializeLeadRow(row) {
  return {
    id: row.id,
    jobId: row.job_id,
    osmType: row.osm_type,
    osmId: row.osm_id,
    name: row.name,
    category: row.category,
    subcategory: row.subcategory,
    website: row.website,
    phone: row.phone,
    email: row.email,
    address: row.address,
    lat: row.lat,
    lon: row.lon,
    sourceBBox: JSON.parse(row.source_bbox_json),
    tags: JSON.parse(row.tags_json),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function defaultSyncState(jobId) {
  return {
    jobId,
    lastSyncedLeadId: 0,
    lastSyncedAt: null,
    lastStatus: "idle",
    lastMessage: null,
    lastStartedAt: null,
    lastFinishedAt: null,
    syncedRecordCount: 0,
  };
}

function deserializeSyncStateRow(row) {
  return {
    jobId: row.job_id,
    lastSyncedLeadId: row.last_synced_lead_id || 0,
    lastSyncedAt: row.last_synced_at,
    lastStatus: row.last_status,
    lastMessage: row.last_message,
    lastStartedAt: row.last_started_at,
    lastFinishedAt: row.last_finished_at,
    syncedRecordCount: row.synced_record_count || 0,
  };
}

function sanitizeNocoDbConfig(input) {
  const autoSyncIntervalMinutes = Number.parseInt(
    input.autoSyncIntervalMinutes,
    10
  );

  return {
    baseUrl: cleanString(input.baseUrl),
    apiToken: cleanString(input.apiToken),
    baseId: cleanString(input.baseId),
    tableId: cleanString(input.tableId),
    autoSyncOnCompletion: Boolean(input.autoSyncOnCompletion),
    autoSyncIntervalMinutes:
      Number.isFinite(autoSyncIntervalMinutes) && autoSyncIntervalMinutes > 0
        ? autoSyncIntervalMinutes
        : 0,
    autoCreateColumns:
      input.autoCreateColumns == null ? true : Boolean(input.autoCreateColumns),
  };
}

function normalizeStringArray(input) {
  const values = Array.isArray(input)
    ? input
    : String(input || "")
        .split(",")
        .map((item) => item.trim());

  return [...new Set(values.filter(Boolean))];
}

function cleanString(value) {
  const normalized = String(value || "").trim();
  return normalized || null;
}

function parseJsonOrFallback(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

module.exports = {
  createStore,
};
