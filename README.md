# OSM Country Scraper

Autonomous country-scale OpenStreetMap scraper built for long-running public API jobs.

## What it does

- accepts a `country + keyword`
- resolves the country once with **Nominatim**
- shards the resolved country geometry into resumable work units instead of relying on one oversized root bbox
- extracts POIs from **Overpass API**
- persists job, shard, and lead state in SQLite
- retries and splits failed or overloaded shards
- re-queues stale claimed shards automatically if a worker claim is orphaned
- lets operators pause and resume long-running jobs without losing state
- exports CSV and JSON artifacts when a job finishes
- exposes a built-in operator dashboard for long-running country jobs
- can sync normalized lead output into NocoDB from the dashboard, automatically on job completion, or incrementally while jobs are still running

## Keyword input

The dashboard/API now accepts any free-text keyword.

- Common hospitality keywords such as `restaurants`, `hotels`, `cafe`, `bar`, `hostel`, and `guest_house` still use exact built-in mappings.
- Any other keyword falls back to broad matching across business-related OSM keys such as `amenity`, `tourism`, `shop`, `office`, `craft`, `healthcare`, `leisure`, `sport`, and `cuisine`.
- You can also force exact or regex selector logic directly in the keyword field with `key=value` or `key~regex`, or send a custom `selectors` array over the API.

Example custom selectors:

```json
{
  "country": "United States",
  "keyword": "tourism=hotel, tourism=guest_house",
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

The dashboard is protected by a username/password login screen backed by server-side sessions in SQLite.

### Custom domain

The intended production hostname is:

`https://osm.luxeillum.com`

If you change domains in Coolify, also update `PUBLIC_BASE_URL` so generated dashboard and API links stay correct.

### Create job

```bash
curl -X POST http://localhost:3000/jobs \
  -b cookies.txt \
  -H 'Content-Type: application/json' \
  -d '{"country":"United States","keyword":"restaurants"}'
```

### Get job

```bash
curl -b cookies.txt http://localhost:3000/jobs/<jobId>
```

This now includes derived monitoring stats such as shard state counts, recent lead growth, and throughput.

### Get job stats

```bash
curl -b cookies.txt http://localhost:3000/jobs/<jobId>/stats
```

### List shards

```bash
curl -b cookies.txt 'http://localhost:3000/jobs/<jobId>/shards?limit=50&offset=0'
curl -b cookies.txt 'http://localhost:3000/jobs/<jobId>/shards?status=retry&limit=50&offset=0'
```

### Recent shard errors

```bash
curl -b cookies.txt 'http://localhost:3000/jobs/<jobId>/errors?limit=25'
```

### List leads

```bash
curl -b cookies.txt 'http://localhost:3000/jobs/<jobId>/leads?limit=100&offset=0'
```

### Download artifacts

```bash
curl -b cookies.txt -L 'http://localhost:3000/jobs/<jobId>/download?format=csv' -o leads.csv
curl -b cookies.txt -L 'http://localhost:3000/jobs/<jobId>/download?format=json' -o leads.json
```

### Cancel job

```bash
curl -b cookies.txt -X POST http://localhost:3000/jobs/<jobId>/cancel
```

Canceling a job moves all pending, retrying, and currently claimed shards into `canceled` and prevents any in-flight shard from writing more results after the cancel request lands.

### Pause or resume job

```bash
curl -b cookies.txt -X POST http://localhost:3000/jobs/<jobId>/pause
curl -b cookies.txt -X POST http://localhost:3000/jobs/<jobId>/resume
```

Pausing stops the scheduler from claiming new shards for that job. Any shard that is already running is allowed to finish safely, and the remaining queued shards stay available for resume later.

### Recover failed shards as smaller tiles

```bash
curl -b cookies.txt -X POST http://localhost:3000/jobs/<jobId>/recover-failed \
  -H 'Content-Type: application/json' \
  -d '{"splitLevels":2}'
```

This operator action is intended for terminal `partial` or `failed` jobs that exhausted retries on a small set of hard shards. It converts each failed shard into smaller child shards, clears stale finished artifacts, and reopens the job so the worker can continue from the recovered tiles.

- `splitLevels` defaults to `2`, which creates `16` child tiles per failed shard.
- Valid values are `1` to `3`.
- Only jobs with at least one `failed` shard can use this action.

### Delete job

```bash
curl -b cookies.txt -X DELETE http://localhost:3000/jobs/<jobId>
```

Only terminal jobs can be deleted. If a job is still running, cancel it first. Deletion removes the job row, its shards, leads, NocoDB sync state, and generated artifact files.

### NocoDB integration

The dashboard includes an **NocoDB integration** panel where you can:

- save the NocoDB base URL, API token, base ID, and table ID
- test the connection before syncing
- enable automatic sync when jobs finish
- set an incremental sync interval in minutes for running jobs (`0` disables in-progress sync)
- manually sync any selected job into NocoDB

The sync writes a normalized lead schema into the chosen table, including:

- `job_id`, `country`, `country_name`, `country_code`, `keyword`, `job_status`
- `osm_type`, `osm_id`, `osm_url`
- `name`, `category`, `subcategory`, `website`, `phone`, `email`, `address`
- `city`, `area`, `state_region`, `postcode`, `lead_country`
- `lat`, `lon`
- `raw_tags_json`, `source_bbox_json`
- `scraped_at`, `lead_created_at`, `lead_updated_at`

If **auto-create columns** is enabled, the scraper will try to create any missing columns in the target table before syncing. Website, phone, email, address, city/area/state/postcode fields, category, and location fields are always part of the default synced schema.

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
- `OVERPASS_QUERY_TIMEOUT_SEC` query timeout passed to Overpass (default `75`)
- `RUNNING_SHARD_STALE_MS` reclaim `running` shards that have been stuck past this timeout (defaults to a value safely above the Overpass timeout)
- `MAX_SHARD_DEPTH` max bbox subdivision depth
- `RETRY_LIMIT` retry attempts before final failure or forced split
- `RETRY_BASE_DELAY_MS` exponential backoff base
- `RESULT_SPLIT_THRESHOLD` split shards if a successful shard returns at least this many elements
- `MIN_SHARD_WIDTH_DEG` and `MIN_SHARD_HEIGHT_DEG` guard against endless subdivision (default `0.05`)
- `SEED_SHARD_MAX_AREA_DEG_SQ` geometry-aware seed shard target size for large countries
- `SEED_SHARD_MAX_DEPTH` max recursive split depth while creating initial geometry-clipped seed shards
- `PRE_QUERY_SPLIT_AREA_DEG_SQ` split oversized shards before the first Overpass query (default `10`)
- `IMMEDIATE_SPLIT_DEPTH` shallow shard depth that should split immediately on 429/504 or timeout pressure (default `4`)
- `ADMIN_USERNAME` dashboard admin username
- `ADMIN_PASSWORD` dashboard admin password
- `SESSION_COOKIE_NAME` cookie name for authenticated sessions
- `SESSION_TTL_HOURS` session lifetime in hours
- `NOCODB_BASE_URL` default NocoDB base URL
- `NOCODB_API_TOKEN` default NocoDB API token
- `NOCODB_BASE_ID` default NocoDB base/project ID
- `NOCODB_TABLE_ID` default NocoDB table ID for synced leads
- `NOCODB_AUTO_SYNC_ON_COMPLETION` automatically sync completed jobs to NocoDB
- `NOCODB_AUTO_SYNC_INTERVAL_MINUTES` sync new leads to NocoDB every N minutes while a job is running (default `30`, `0` disables)
- `NOCODB_AUTO_CREATE_COLUMNS` create missing target columns before syncing

## Run locally

```bash
npm install
ADMIN_USERNAME=admin ADMIN_PASSWORD=secret123 PORT=8092 node index.js
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
- **Generated links:** set `PUBLIC_BASE_URL=https://osm.luxeillum.com`
- **Dashboard auth:** set `ADMIN_USERNAME` and `ADMIN_PASSWORD`
- **Optional NocoDB defaults:** set `NOCODB_*` env vars if you want the dashboard pre-filled on first boot

## Notes

- This service is designed for **public API best-effort scraping**, not guaranteed throughput.
- Country-scale public Overpass runs can take days or weeks.
- Large countries are now seeded from geometry-clipped initial shards so countries like the United States do not start from a world-spanning bbox.
- Website values come from OSM tags such as `website` and `contact:website`.
- Long-running progress is best interpreted through shard states rather than only job status. A country job can keep splitting into finer shards as dense areas are discovered.
- If a shard ever gets orphaned in `running`, the worker now re-queues it automatically once it exceeds the stale timeout instead of waiting for a process restart.
- If a job ends `partial` because a few shards exhausted retries, operators can now reopen only the failed shards as smaller tiles instead of rerunning the whole job.
- The dashboard and job APIs are intentionally protected behind the same login so the UI cannot be bypassed by unauthenticated requests.
- NocoDB is treated as an output/sync layer, not the scraper's authoritative runtime database.
