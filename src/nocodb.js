const STANDARD_FIELDS = [
  { name: "job_id", type: "SingleLineText" },
  { name: "country", type: "SingleLineText" },
  { name: "country_name", type: "SingleLineText" },
  { name: "country_code", type: "SingleLineText" },
  { name: "keyword", type: "SingleLineText" },
  { name: "job_status", type: "SingleLineText" },
  { name: "osm_type", type: "SingleLineText" },
  { name: "osm_id", type: "SingleLineText" },
  { name: "osm_url", type: "URL" },
  { name: "name", type: "SingleLineText" },
  { name: "category", type: "SingleLineText" },
  { name: "subcategory", type: "SingleLineText" },
  { name: "website", type: "URL" },
  { name: "phone", type: "PhoneNumber" },
  { name: "email", type: "Email" },
  { name: "address", type: "LongText" },
  { name: "lat", type: "Number" },
  { name: "lon", type: "Number" },
  { name: "raw_tags_json", type: "LongText" },
  { name: "source_bbox_json", type: "LongText" },
  { name: "scraped_at", type: "DateTime" },
  { name: "lead_created_at", type: "DateTime" },
  { name: "lead_updated_at", type: "DateTime" },
];

function createNocoDbService({ store, config }) {
  return {
    getConfig() {
      return toPublicConfig(store.getNocoDbConfig());
    },

    saveConfig(input) {
      const saved = store.saveNocoDbConfig(input);
      return toPublicConfig(saved);
    },

    async testConnection(input = null) {
      const settings = resolveSettings(store, input);
      validateSettings(settings);

      const columns = await listColumns(settings);
      return {
        ok: true,
        tableId: settings.tableId,
        columnCount: columns.length,
        autoSyncOnCompletion: settings.autoSyncOnCompletion,
        autoSyncIntervalMinutes: settings.autoSyncIntervalMinutes || 0,
        autoCreateColumns: settings.autoCreateColumns,
      };
    },

    getJobSyncStatus(jobId) {
      return {
        enabled: hasEnoughSettings(store.getNocoDbConfig()),
        config: toPublicConfig(store.getNocoDbConfig()),
        sync: store.getNocoDbSyncState(jobId),
      };
    },

    async syncJob(jobId, options = {}) {
      const settings = resolveSettings(store, options.config);
      validateSettings(settings);

      const job = store.getJob(jobId);
      if (!job) {
        throw createHttpError(404, "Job not found.");
      }

      store.markNocoDbSyncStarted(jobId);

      try {
        const desiredFields = buildDesiredFields(settings.promotedTags);
        let columns = await listColumns(settings);
        let availableFields = collectColumnNames(columns);

        if (settings.autoCreateColumns) {
          const missingFields = desiredFields.filter(
            (field) => !availableFields.has(field.name)
          );

          for (const field of missingFields) {
            await createColumn(settings, field);
          }

          if (missingFields.length > 0) {
            columns = await listColumns(settings);
            availableFields = collectColumnNames(columns);
          }
        }

        const syncState = options.force
          ? defaultSyncState(jobId)
          : store.getNocoDbSyncState(jobId);

        let lastSyncedLeadId = options.force ? 0 : syncState.lastSyncedLeadId;
        let syncedRecordCount = 0;

        while (true) {
          const leads = store.getJobLeadsAfterId(jobId, lastSyncedLeadId, {
            limit: 100,
          });

          if (leads.length === 0) {
            break;
          }

          const records = leads.map((lead) =>
            buildRecord(job, lead, settings.promotedTags, availableFields)
          );

          await createRecords(settings, records);
          syncedRecordCount += records.length;
          lastSyncedLeadId = leads[leads.length - 1].id;
        }

        const message = syncedRecordCount
          ? `Synced ${syncedRecordCount} lead records to NocoDB.`
          : "No new leads to sync.";

        store.markNocoDbSyncSuccess(jobId, {
          lastSyncedLeadId,
          syncedRecordCount,
          message,
        });

        return {
          ok: true,
          jobId,
          syncedRecordCount,
          config: toPublicConfig(settings),
          sync: store.getNocoDbSyncState(jobId),
        };
      } catch (error) {
        store.markNocoDbSyncFailure(jobId, error.message);
        throw error;
      }
    },

    async syncCompletedJobIfEnabled(jobId) {
      const settings = store.getNocoDbConfig();
      if (!settings.autoSyncOnCompletion || !hasEnoughSettings(settings)) {
        return null;
      }

      try {
        return await this.syncJob(jobId);
      } catch (error) {
        console.error(`NocoDB sync failed for job ${jobId}:`, error.message);
        return null;
      }
    },

    getRunningJobSyncIdsDue() {
      const settings = store.getNocoDbConfig();
      if (!hasEnoughSettings(settings)) {
        return [];
      }

      const intervalMinutes = settings.autoSyncIntervalMinutes || 0;
      if (intervalMinutes <= 0) {
        return [];
      }

      const intervalMs = intervalMinutes * 60 * 1000;
      const now = Date.now();

      return store
        .listJobs({ limit: 250 })
        .filter((job) => ["queued", "running"].includes(job.status))
        .map((job) => ({
          job,
          syncState: store.getNocoDbSyncState(job.id),
        }))
        .filter(({ syncState }) => syncState.lastStatus !== "running")
        .filter(({ job, syncState }) => {
          const hasUnsyncedLead =
            store.getJobLeadsAfterId(job.id, syncState.lastSyncedLeadId || 0, {
              limit: 1,
            }).length > 0;

          if (!hasUnsyncedLead) {
            return false;
          }

          const lastActivityAt =
            syncState.lastFinishedAt || syncState.lastStartedAt || null;

          if (!lastActivityAt) {
            return true;
          }

          return now - Date.parse(lastActivityAt) >= intervalMs;
        })
        .map(({ job }) => job.id);
    },
  };
}

function resolveSettings(store, input) {
  if (!input) {
    return store.getNocoDbConfig();
  }

  const current = store.getNocoDbConfig();
  return sanitizeSettings({
    ...current,
    ...input,
    apiToken:
      input.apiToken == null || input.apiToken === ""
        ? current.apiToken
        : input.apiToken,
  });
}

function sanitizeSettings(input) {
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
    promotedTags: normalizeStringArray(input.promotedTags),
  };
}

function validateSettings(settings) {
  if (!hasEnoughSettings(settings)) {
    throw createHttpError(
      400,
      "NocoDB base URL, API token, base ID, and table ID are required."
    );
  }
}

function hasEnoughSettings(settings) {
  return Boolean(
    settings.baseUrl &&
      settings.apiToken &&
      settings.baseId &&
      settings.tableId
  );
}

function toPublicConfig(settings) {
  return {
    baseUrl: settings.baseUrl,
    baseId: settings.baseId,
    tableId: settings.tableId,
    autoSyncOnCompletion: Boolean(settings.autoSyncOnCompletion),
    autoSyncIntervalMinutes: settings.autoSyncIntervalMinutes || 0,
    autoCreateColumns: settings.autoCreateColumns !== false,
    promotedTags: normalizeStringArray(settings.promotedTags),
    hasApiToken: Boolean(settings.apiToken),
  };
}

function buildDesiredFields(promotedTags) {
  const extraFields = normalizeStringArray(promotedTags).map((tagKey) => ({
    name: promotedTagFieldName(tagKey),
    type: "SingleLineText",
  }));

  const byName = new Map();
  for (const field of [...STANDARD_FIELDS, ...extraFields]) {
    byName.set(field.name, field);
  }

  return [...byName.values()];
}

function buildRecord(job, lead, promotedTags, availableFields) {
  const record = {
    job_id: job.id,
    country: job.country,
    country_name: job.countryName || job.country,
    country_code: job.countryCode || "",
    keyword: job.keyword,
    job_status: job.status,
    osm_type: lead.osmType,
    osm_id: lead.osmId,
    osm_url: `https://www.openstreetmap.org/${lead.osmType}/${lead.osmId}`,
    name: lead.name || "",
    category: lead.category || "",
    subcategory: lead.subcategory || "",
    website: lead.website || "",
    phone: lead.phone || "",
    email: lead.email || "",
    address: lead.address || "",
    lat: lead.lat,
    lon: lead.lon,
    raw_tags_json: JSON.stringify(lead.tags || {}),
    source_bbox_json: JSON.stringify(lead.sourceBBox || null),
    scraped_at: lead.updatedAt || lead.createdAt || new Date().toISOString(),
    lead_created_at: lead.createdAt || null,
    lead_updated_at: lead.updatedAt || null,
  };

  for (const tagKey of normalizeStringArray(promotedTags)) {
    record[promotedTagFieldName(tagKey)] = stringOrEmpty(lead.tags?.[tagKey]);
  }

  return Object.fromEntries(
    Object.entries(record).filter(([fieldName]) => availableFields.has(fieldName))
  );
}

async function listColumns(settings) {
  const payload = await apiRequestFallback(settings, [
    {
      pathname: `/api/v2/meta/tables/${encodeURIComponent(settings.tableId)}/columns`,
    },
    {
      pathname: `/api/v1/db/meta/tables/${encodeURIComponent(settings.tableId)}`,
      transform: (result) => result?.columns || [],
    },
  ]);

  if (Array.isArray(payload)) {
    return payload;
  }

  if (Array.isArray(payload?.list)) {
    return payload.list;
  }

  return [];
}

async function createColumn(settings, field) {
  const payload = {
    title: field.name,
    column_name: field.name,
    name: field.name,
    uidt: field.type,
    type: field.type,
  };

  return apiRequestFallback(settings, [
    {
      pathname: `/api/v2/base/${encodeURIComponent(settings.baseId)}/table/${encodeURIComponent(
        settings.tableId
      )}/column`,
      method: "POST",
      body: payload,
    },
    {
      pathname: `/api/v1/db/meta/tables/${encodeURIComponent(settings.tableId)}/columns`,
      method: "POST",
      body: payload,
    },
  ]);
}

async function createRecords(settings, records) {
  if (!records.length) {
    return null;
  }

  return apiRequestFallback(settings, [
    {
      pathname: `/api/v2/tables/${encodeURIComponent(settings.tableId)}/records`,
      method: "POST",
      body: records,
    },
    {
      pathname: `/api/v1/db/data/noco/${encodeURIComponent(settings.baseId)}/${encodeURIComponent(
        settings.tableId
      )}`,
      method: "POST",
      body: records,
    },
    {
      pathname: `/api/v1/db/data/noco/${encodeURIComponent(settings.baseId)}/${encodeURIComponent(
        settings.tableId
      )}`,
      method: "POST",
      body: { list: records },
    },
  ]);
}

async function apiRequest(settings, pathname, options = {}) {
  const response = await fetch(joinUrl(settings.baseUrl, pathname), {
    method: options.method || "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "xc-auth": settings.apiToken,
      "xc-token": settings.apiToken,
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
  });

  const text = await response.text();
  const payload = text ? safeJsonParse(text) : null;

  if (!response.ok) {
    const message =
      payload?.msg ||
      payload?.message ||
      payload?.error ||
      `NocoDB request failed with status ${response.status}.`;
    throw createHttpError(response.status, message);
  }

  return payload;
}

async function apiRequestFallback(settings, attempts) {
  let lastError = null;

  for (const attempt of attempts) {
    try {
      const result = await apiRequest(settings, attempt.pathname, attempt);
      return typeof attempt.transform === "function"
        ? attempt.transform(result)
        : result;
    } catch (error) {
      lastError = error;
      if (![400, 404].includes(error.statusCode)) {
        throw error;
      }
    }
  }

  throw lastError || createHttpError(500, "NocoDB request failed.");
}

function joinUrl(baseUrl, pathname) {
  return `${String(baseUrl || "").replace(/\/+$/, "")}${pathname}`;
}

function collectColumnNames(columns) {
  const names = new Set();
  for (const column of columns) {
    const candidates = [
      column.column_name,
      column.name,
      column.title,
      column.displayName,
    ];
    for (const value of candidates) {
      if (value) {
        names.add(String(value));
      }
    }
  }
  return names;
}

function promotedTagFieldName(tagKey) {
  return `osm_${String(tagKey)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")}`;
}

function normalizeStringArray(input) {
  const values = Array.isArray(input)
    ? input
    : String(input || "")
        .split(",")
        .map((item) => item.trim());

  return [...new Set(values.filter(Boolean))];
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

function cleanString(value) {
  const normalized = String(value || "").trim();
  return normalized || null;
}

function stringOrEmpty(value) {
  return value == null ? "" : String(value);
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch {
    return { message: value };
  }
}

function createHttpError(statusCode, message) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

module.exports = {
  createNocoDbService,
  promotedTagFieldName,
};
