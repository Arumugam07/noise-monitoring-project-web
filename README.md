## Noise Monitoring ETL (API → Supabase)

Beginner-friendly, minimal pipeline that fetches meter readings directly from the API and stores them in Supabase (PostgreSQL). Optimized for clarity and reliability.

### What you get
- **Simple flow**: API → Python → Supabase
- **Two modes**: daily (yesterday) and backfill (date range)
- **Safe upsert**: no duplicates, unique per minute per device
- **Timezone-correct**: Singapore Time input, stored in UTC

### Architecture
API → ETL Script (`fetch_meter_data_bq.py`) → Supabase table `meter_readings`

---

## 1) Setup

### a) Install
```bash
pip install -r requirements.txt
```

### b) Configure environment
Copy `.env.example` to `.env` and fill in values:
```bash
cp .env.example .env
```

Required values:
- SUPABASE_URL
- SUPABASE_ANON_KEY (or service role key)
- API_BASE_URL (default is provided)

### c) Create table (run once in Supabase SQL editor)
```sql
CREATE TABLE IF NOT EXISTS public.meter_readings (
  location_id text NOT NULL,
  location_name text,
  reading_value double precision,
  reading_datetime timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT meter_readings_pkey PRIMARY KEY (location_id, reading_datetime)
);
CREATE INDEX IF NOT EXISTS idx_meter_readings_datetime ON public.meter_readings (reading_datetime);
```

---

## 2) Usage

### a) Daily (yesterday only)
```bash
python fetch_meter_data_bq.py daily
```

### b) Backfill a range (inclusive)
```bash
python fetch_meter_data_bq.py backfill 2024-03-01 2024-04-15
```

---

## 3) How it works

- Queries API per device per day: `GET /api/meter-sound/{id}?start=YYYY-MM-DD`
- Builds exact Singapore calendar day, converts to UTC per-minute
- Upserts rows into Supabase with composite PK `(location_id, reading_datetime)`
- Skips future timestamps and duplicates

---

## 4) Troubleshooting

- No rows inserted: confirm API returns data for the dates
- Duplicates: primary key prevents them; upsert ensures idempotency
- Timezone: inputs are SGT, storage is UTC; convert in queries for display

---

## 5) Configuration reference

- SUPABASE_URL: your project URL
- SUPABASE_ANON_KEY: anon or service role key
- API_BASE_URL: default `http://139.59.223.231:3000/api/meter-sound`

- **Coverage**: 24-hour continuous monitoring (7 AM to 6:59 AM next day)
- **Values**: Sound levels typically 20-90 dB
- **Validation**: Automatic data type conversion and null handling

## Monitoring

### Success Indicators
- GitHub Actions workflow completes successfully
- Data appears in Supabase database
- No GitHub issues created

### Troubleshooting
Check GitHub Actions logs for:
- API connection errors
- Database connection issues
- Data processing failures

Common issues:
- **API timeout**: Increase timeout or add retry logic
- **Database conflicts**: Handled automatically with upsert
- **Missing data**: Some dates may have no sensor data

## Database Queries

### Recent Data
```sql
SELECT * FROM meter_readings 
WHERE reading_datetime >= NOW() - INTERVAL '24 hours'
ORDER BY reading_datetime DESC;
```

### Daily Averages
```sql
SELECT 
    location_id,
    DATE(reading_datetime) as date,
    AVG(reading_value) as avg_reading,
    MIN(reading_value) as min_reading,
    MAX(reading_value) as max_reading
FROM meter_readings 
WHERE reading_datetime >= NOW() - INTERVAL '30 days'
GROUP BY location_id, DATE(reading_datetime)
ORDER BY date DESC;
```

### Hourly Patterns
```sql
SELECT 
    EXTRACT(hour FROM reading_datetime) as hour,
    AVG(reading_value) as avg_reading
FROM meter_readings 
WHERE reading_datetime >= NOW() - INTERVAL '7 days'
GROUP BY EXTRACT(hour FROM reading_datetime)
ORDER BY hour;
```

## Cost Estimation

**Supabase Pro Plan ($25/month):**
- Storage: ~50MB per month (well within 100GB limit)
- Compute: Micro instance included
- Expected cost: $25-30/month total

**GitHub Actions:**
- ~30 minutes/month (well within free tier)
- Cost: $0

## Contributing

1. Test changes locally first
2. Update this README if adding features
3. Ensure all tests pass before merging

## License

MIT License - feel free to modify for your needs.