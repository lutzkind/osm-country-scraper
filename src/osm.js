const turf = require("@turf/turf");
const {
  parseBoundingBox,
  pointInsideGeometry,
} = require("./geo");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildOverpassQuery(bbox, selectors, timeoutSeconds) {
  const fragments = selectors.flatMap((selector) => {
    const escapedKey = selector.key.replace(/"/g, '\\"');
    const escapedValue = selector.value.replace(/"/g, '\\"');
    const bboxClause = `(${bbox.south},${bbox.west},${bbox.north},${bbox.east})`;
    const valueClause =
      selector.match === "regex"
        ? `["${escapedKey}"~"${escapedValue}",i]`
        : `["${escapedKey}"="${escapedValue}"]`;

    return [
      `node${valueClause}${bboxClause};`,
      `way${valueClause}${bboxClause};`,
      `relation${valueClause}${bboxClause};`,
    ];
  });

  return `[out:json][timeout:${timeoutSeconds}];(${fragments.join("")});out tags center qt;`;
}

async function resolveCountry(country, config) {
  const params = new URLSearchParams({
    country,
    format: "jsonv2",
    limit: "1",
    featuretype: "country",
    polygon_geojson: "1",
  });

  const response = await fetch(`${config.nominatimUrl}?${params.toString()}`, {
    headers: {
      "User-Agent": config.userAgent,
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(
      `Nominatim returned ${response.status} while resolving "${country}".`
    );
  }

  const payload = await response.json();
  const first = payload[0];

  if (!first) {
    const error = new Error(`Country "${country}" could not be resolved.`);
    error.statusCode = 404;
    throw error;
  }

  const geometry = first.geojson ? turf.feature(first.geojson) : null;

  return {
    displayName: first.display_name,
    countryCode: first.address?.country_code || null,
    bbox: parseBoundingBox(first.boundingbox),
    geometry,
    raw: first,
  };
}

let overpassThrottle = Promise.resolve();

function queueOverpassRequest(task, delayMs) {
  const run = overpassThrottle.then(task);
  overpassThrottle = run
    .catch(() => undefined)
    .then(() => sleep(delayMs));
  return run;
}

async function queryOverpass({ bbox, selectors, geometry, config }) {
  return queueOverpassRequest(async () => {
    const query = buildOverpassQuery(
      bbox,
      selectors,
      config.overpassQueryTimeoutSec
    );

    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      config.overpassTimeoutMs
    );

    try {
      const response = await fetch(config.overpassUrl, {
        method: "POST",
        headers: {
          "User-Agent": config.userAgent,
          "Content-Type": "text/plain",
          Accept: "application/json",
        },
        body: query,
        signal: controller.signal,
      });

      if (!response.ok) {
        const text = await response.text();
        const error = new Error(
          `Overpass returned ${response.status}: ${text.slice(0, 400)}`
        );
        error.statusCode = response.status;
        throw error;
      }

      const payload = await response.json();
      const leads = (payload.elements || [])
        .map((element) => mapElementToLead(element, geometry, bbox))
        .filter(Boolean);

      return { leads, rawCount: payload.elements?.length || 0 };
    } finally {
      clearTimeout(timeout);
    }
  }, config.overpassDelayMs);
}

function normalizeWebsite(value) {
  if (!value) {
    return "";
  }

  let input = String(value).trim();
  if (!input) {
    return "";
  }

  if (!/^https?:\/\//i.test(input)) {
    input = `https://${input}`;
  }

  try {
    const url = new URL(input);
    url.hash = "";
    if (url.pathname === "/") {
      url.pathname = "";
    }
    return url.toString().replace(/\/$/, "");
  } catch {
    return "";
  }
}

function normalizePhone(value) {
  return value ? String(value).trim() : "";
}

function normalizeEmail(value) {
  return value ? String(value).trim().toLowerCase() : "";
}

function buildAddress(tags) {
  if (tags["addr:full"]) {
    return tags["addr:full"];
  }

  const location = extractLocationFields(tags);
  const parts = [
    [tags["addr:housenumber"], tags["addr:street"]].filter(Boolean).join(" "),
    location.area,
    location.city,
    location.stateRegion,
    location.postcode,
    location.country,
  ].filter(Boolean);

  return parts.join(", ");
}

function extractLocationFields(tags) {
  return {
    city: pickTagValue(tags, [
      "addr:city",
      "addr:town",
      "addr:village",
      "addr:municipality",
      "addr:hamlet",
    ]),
    area: pickTagValue(tags, [
      "addr:suburb",
      "addr:district",
      "addr:city_district",
      "addr:neighbourhood",
      "addr:neighborhood",
      "addr:quarter",
      "addr:county",
    ]),
    stateRegion: pickTagValue(tags, [
      "addr:state",
      "addr:province",
      "addr:region",
      "addr:state_district",
    ]),
    postcode: pickTagValue(tags, ["addr:postcode"]),
    country: pickTagValue(tags, ["addr:country", "is_in:country"]),
  };
}

function pickTagValue(tags, keys) {
  for (const key of keys) {
    const value = String(tags[key] || "").trim();
    if (value) {
      return value;
    }
  }
  return "";
}

function mapElementToLead(element, geometry, bbox) {
  const tags = element.tags || {};
  const lat = element.center?.lat ?? element.lat;
  const lon = element.center?.lon ?? element.lon;

  if (lat == null || lon == null) {
    return null;
  }

  if (!pointInsideGeometry(lat, lon, geometry)) {
    return null;
  }

  const location = extractLocationFields(tags);

  return {
    osmType: element.type,
    osmId: String(element.id),
    name: tags.name || "",
    category:
      tags.amenity ||
      tags.tourism ||
      tags.shop ||
      tags.office ||
      tags.craft ||
      tags.healthcare ||
      tags.leisure ||
      tags.sport ||
      "",
    subcategory:
      tags.cuisine ||
      tags["tourism:type"] ||
      tags.brand ||
      tags.operator ||
      "",
    website: normalizeWebsite(tags.website || tags["contact:website"]),
    phone: normalizePhone(tags.phone || tags["contact:phone"]),
    email: normalizeEmail(tags.email || tags["contact:email"]),
    address: buildAddress(tags),
    city: location.city,
    area: location.area,
    stateRegion: location.stateRegion,
    postcode: location.postcode,
    country: location.country,
    lat,
    lon,
    bbox,
    tags,
  };
}

module.exports = {
  resolveCountry,
  queryOverpass,
  normalizeWebsite,
};
