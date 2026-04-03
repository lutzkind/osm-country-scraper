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

function normalizeKeyword(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function resolveSelectors(keyword, selectors) {
  if (Array.isArray(selectors) && selectors.length > 0) {
    return selectors.map((selector) => ({
      key: selector.key,
      value: selector.value,
    }));
  }

  const normalized = normalizeKeyword(keyword);
  const mapping = KEYWORD_MAPPINGS[normalized];
  if (!mapping) {
    const supported = Object.keys(KEYWORD_MAPPINGS).sort();
    const error = new Error(
      `Unsupported keyword "${keyword}". Supported keywords: ${supported.join(
        ", "
      )}`
    );
    error.statusCode = 400;
    throw error;
  }

  return mapping;
}

module.exports = {
  KEYWORD_MAPPINGS,
  resolveSelectors,
};
