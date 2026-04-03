const KEYWORD_MAPPINGS = {
  restaurants: [{ key: "amenity", value: "restaurant" }],
  restaurant: [{ key: "amenity", value: "restaurant" }],
  hotels: [
    { key: "tourism", value: "hotel" },
    { key: "tourism", value: "guest_house" },
    { key: "tourism", value: "hostel" },
    { key: "tourism", value: "motel" },
    { key: "tourism", value: "apartment" },
  ],
  hotel: [
    { key: "tourism", value: "hotel" },
    { key: "tourism", value: "guest_house" },
    { key: "tourism", value: "hostel" },
    { key: "tourism", value: "motel" },
    { key: "tourism", value: "apartment" },
  ],
  cafes: [{ key: "amenity", value: "cafe" }],
  cafe: [{ key: "amenity", value: "cafe" }],
  bars: [{ key: "amenity", value: "bar" }],
  bar: [{ key: "amenity", value: "bar" }],
  hostels: [{ key: "tourism", value: "hostel" }],
  hostel: [{ key: "tourism", value: "hostel" }],
  "guest houses": [{ key: "tourism", value: "guest_house" }],
  guest_house: [{ key: "tourism", value: "guest_house" }],
};

const FALLBACK_KEYS = [
  "amenity",
  "tourism",
  "shop",
  "office",
  "craft",
  "healthcare",
  "leisure",
  "sport",
  "cuisine",
];

function normalizeKeyword(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function singularizeToken(token) {
  if (token.endsWith("ies") && token.length > 3) {
    return `${token.slice(0, -3)}y`;
  }
  if (token.endsWith("ses") && token.length > 3) {
    return token.slice(0, -2);
  }
  if (token.endsWith("s") && !token.endsWith("ss") && token.length > 3) {
    return token.slice(0, -1);
  }
  return token;
}

function buildKeywordVariants(keyword) {
  const normalized = normalizeKeyword(keyword).replace(/[_-]+/g, " ");
  const tokens = normalized.split(/\s+/).filter(Boolean);
  const variants = new Set();

  if (normalized) {
    variants.add(normalized);
  }

  for (const token of tokens) {
    variants.add(token);
    variants.add(singularizeToken(token));
  }

  if (tokens.length > 1) {
    variants.add(tokens.map((token) => singularizeToken(token)).join(" "));
    variants.add(tokens[tokens.length - 1]);
    variants.add(singularizeToken(tokens[tokens.length - 1]));
  }

  return [...variants].filter(Boolean);
}

function buildRegexPattern(keyword) {
  const fragments = buildKeywordVariants(keyword).map((value) =>
    value
      .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
      .replace(/\s+/g, "[_\\s-]*")
  );

  return `(^|[_\\s-])(?:${[...new Set(fragments)].join("|")})(?:$|[_\\s-])`;
}

function parseExplicitSelectors(keyword) {
  const raw = String(keyword || "");
  const selectors = raw
    .split(/[\n,]+/)
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .map((chunk) => {
      const regexMatch = chunk.match(/^([a-zA-Z0-9:_-]+)\s*~\s*(.+)$/);
      if (regexMatch) {
        return {
          key: regexMatch[1],
          value: regexMatch[2].trim(),
          match: "regex",
        };
      }

      const exactMatch = chunk.match(/^([a-zA-Z0-9:_-]+)\s*=\s*(.+)$/);
      if (exactMatch) {
        return {
          key: exactMatch[1],
          value: exactMatch[2].trim(),
        };
      }

      return null;
    })
    .filter(Boolean);

  return selectors.length ? selectors : null;
}

function buildFallbackSelectors(keyword) {
  const pattern = buildRegexPattern(keyword);
  return FALLBACK_KEYS.map((key) => ({
    key,
    value: pattern,
    match: "regex",
  }));
}

function resolveSelectors(keyword, selectors) {
  if (Array.isArray(selectors) && selectors.length > 0) {
    return selectors.map((selector) => ({
      key: selector.key,
      value: selector.value,
      match: selector.match === "regex" ? "regex" : "exact",
    }));
  }

  const normalized = normalizeKeyword(keyword);
  const mapping = KEYWORD_MAPPINGS[normalized];
  if (mapping) {
    return mapping.map((selector) => ({
      key: selector.key,
      value: selector.value,
      match: "exact",
    }));
  }

  const explicitSelectors = parseExplicitSelectors(keyword);
  if (explicitSelectors) {
    return explicitSelectors;
  }

  return buildFallbackSelectors(keyword);
}

module.exports = {
  KEYWORD_MAPPINGS,
  resolveSelectors,
};
