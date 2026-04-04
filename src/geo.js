const turf = require("@turf/turf");

function parseBoundingBox(rawBoundingBox) {
  if (!Array.isArray(rawBoundingBox) || rawBoundingBox.length !== 4) {
    throw new Error("Invalid country bounding box.");
  }

  const [south, north, west, east] = rawBoundingBox.map((value) =>
    Number.parseFloat(value)
  );

  return { south, west, north, east };
}

function bboxToArray(bbox) {
  return [bbox.west, bbox.south, bbox.east, bbox.north];
}

function splitBBox(bbox) {
  const midLat = (bbox.south + bbox.north) / 2;
  const midLon = (bbox.west + bbox.east) / 2;

  return [
    { south: bbox.south, west: bbox.west, north: midLat, east: midLon },
    { south: bbox.south, west: midLon, north: midLat, east: bbox.east },
    { south: midLat, west: bbox.west, north: bbox.north, east: midLon },
    { south: midLat, west: midLon, north: bbox.north, east: bbox.east },
  ];
}

function bboxAreaDegrees(bbox) {
  return Math.max(0, bbox.east - bbox.west) * Math.max(0, bbox.north - bbox.south);
}

function canSplitBBox(bbox, config) {
  return (
    bbox.east - bbox.west > config.minShardWidthDeg &&
    bbox.north - bbox.south > config.minShardHeightDeg
  );
}

function bboxIntersectsGeometry(bbox, geometry) {
  if (!geometry) {
    return true;
  }

  return turf.booleanIntersects(turf.bboxPolygon(bboxToArray(bbox)), geometry);
}

function pointInsideGeometry(lat, lon, geometry) {
  if (!geometry) {
    return true;
  }

  return turf.booleanPointInPolygon(turf.point([lon, lat]), geometry);
}

function buildSeedBBoxes(bbox, geometry, config, depth = 0) {
  if (!geometry) {
    return [bbox];
  }

  if (!bboxIntersectsGeometry(bbox, geometry)) {
    return [];
  }

  const shouldSplit =
    depth < config.seedShardMaxDepth &&
    canSplitBBox(bbox, config) &&
    bboxAreaDegrees(bbox) > config.seedShardMaxAreaDegSq;

  if (!shouldSplit) {
    return [bbox];
  }

  return splitBBox(bbox).flatMap((child) =>
    buildSeedBBoxes(child, geometry, config, depth + 1)
  );
}

module.exports = {
  parseBoundingBox,
  bboxToArray,
  bboxAreaDegrees,
  splitBBox,
  canSplitBBox,
  bboxIntersectsGeometry,
  pointInsideGeometry,
  buildSeedBBoxes,
};
