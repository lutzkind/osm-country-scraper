const fs = require("fs");
const path = require("path");

function escapeCsv(value) {
  const text = value == null ? "" : String(value);
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function writeArtifacts(store, config, jobId) {
  const leads = store.getJobLeads(jobId, { limit: 1000000000, offset: 0 });
  const targetDir = path.join(config.exportsDir, jobId);
  fs.mkdirSync(targetDir, { recursive: true });

  const csvPath = path.join(targetDir, "leads.csv");
  const jsonPath = path.join(targetDir, "leads.json");

  const headers = [
    "name",
    "category",
    "subcategory",
    "website",
    "phone",
    "email",
    "address",
    "lat",
    "lon",
    "osm_type",
    "osm_id",
  ];

  const csvLines = [
    headers.join(","),
    ...leads.map((lead) =>
      [
        lead.name,
        lead.category,
        lead.subcategory,
        lead.website,
        lead.phone,
        lead.email,
        lead.address,
        lead.lat,
        lead.lon,
        lead.osmType,
        lead.osmId,
      ]
        .map(escapeCsv)
        .join(",")
    ),
  ];

  fs.writeFileSync(csvPath, `${csvLines.join("\n")}\n`, "utf8");
  fs.writeFileSync(jsonPath, JSON.stringify(leads, null, 2), "utf8");

  return { csvPath, jsonPath };
}

module.exports = {
  writeArtifacts,
};
