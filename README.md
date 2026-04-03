# OSM Country Scraper

Autonomous country-scale OpenStreetMap scraper built for long-running public API jobs.

## What it does

- accepts a `country + keyword`
- resolves the country once with **Nominatim**
- shards the country bounding box into resumable work units
- extracts POIs from **Overpass API**
- persists job, shard, and lead state in SQLite
- retries and splits failed or overloaded shards
- exports CSV and JSON artifacts when a job finishes
- exposes a built-in operator dashboard for long-running country jobs

## Supported keywords

- `restaurants`
- `hotels`
- `cafe`
- `bar`
- `hostel`
- `guest_house`

You can also send a custom `selectors` array:

```json
{
  "country": "United States",
  "keyword": "custom",
  "selectors": [
    { "key": "tourism", "value": "hotel" },
    { "key": "tourism", "value": "guest_house" }
  ]
}
```

## API

### Operator dashboard

Open the built-in dashboard in a browser:

`http://localhost:3000/dashboard`

### Create job

```bash
curl -X POST http://localhost:3000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"country":"United States","keyword":"restaurants"}'
```

### Get job

```bash
curl http://localhost:3000/jobs/<jobId>
```

This now includes derived monitoring stats such as shard state counts, recent lead growth, and throughput.

### Get job stats

```bash
curl http://localhost:3000/jobs/<jobId>/stats
```

### List shards

```bash
curl 'http://localhost:3000/jobs/<jobId>/shards?limit=50&offset=0'
curl 'http://localhost:3000/jobs/<jobId>/shards?status=retry&limit=50&offset=0'
```

### Recent shard errors

```bash
curl 'http://localhost:3000/jobs/<jobId>/errors?limit=25'
```

### List leads

```bash
curl 'http://localhost:3000/jobs/<jobId>/leads?limit=100&offset=0'
```

### Download artifacts

```bash
curl -L 'http://localhost:3000/jobs/<jobId>/download?format=csv' -o leads.csv
curl -L 'http://localhost:3000/jobs/<jobId>/download?format=json' -o leads.json
```

### Cancel job

```bash
curl -X POST http://localhost:3000/jobs/<jobId>/cancel
```

## Environment

- `PORT` default `3000`
- `DATA_DIR` default `./data`
- `DB_PATH` default `./data/osm-country-scraper.db`
- `EXPORTS_DIR` default `./data/exports`
- `USER_AGENT` custom identifier for Nominatim/Overpass
- `NOMINATIM_URL` default public Nominatim
- `OVERPASS_URL` default public Overpass
- `WORKER_POLL_MS` worker loop interval
- `OVERPASS_DELAY_MS` delay between Overpass requests
- `OVERPASS_TIMEOUT_MS` HTTP timeout
- `OVERPASS_QUERY_TIMEOUT_SEC` query timeout passed to Overpass
- `MAX_SHARD_DEPTH` max bbox subdivision depth
- `RETRY_LIMIT` retry attempts before final failure or forced split
- `RETRY_BASE_DELAY_MS` exponential backoff base
- `RESULT_SPLIT_THRESHOLD` split shards if a successful shard returns at least this many elements
- `MIN_SHARD_WIDTH_DEG` and `MIN_SHARD_HEIGHT_DEG` guard against endless subdivision

## Run locally

```bash
npm install
PORT=8092 node index.js
```

For public Nominatim usage, set `USER_AGENT` to a real, contactable identifier if you deploy this anywhere else. Public Nominatim can reject generic placeholder identities.

## Docker

```bash
docker build -t osm-country-scraper .
docker run -p 3000:3000 -v $(pwd)/data:/app/data osm-country-scraper
```

## Coolify

- **Build pack:** Dockerfile
- **Port:** `3000`
- **Persistent storage:** mount a writable volume to `/app/data`
- **Recommended env:** set `USER_AGENT` to a real contactable identifier for your deployment

## Notes

- This service is designed for **public API best-effort scraping**, not guaranteed throughput.
- Country-scale public Overpass runs can take days or weeks.
- Website values come from OSM tags such as `website` and `contact:website`.
- Long-running progress is best interpreted through shard states rather than only job status. A country job can keep splitting into finer shards as dense areas are discovered.
