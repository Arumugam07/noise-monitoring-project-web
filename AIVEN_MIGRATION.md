# Finish the Supabase → Aiven migration

The initial database copy is already present in Aiven. Do not run the initial-copy
workflow again. This change moves the application data access to PostgreSQL while
preserving the existing Singapore timezone, reporting-day logic, presets, health
thresholds, and noise-event calculations. Legacy `supabase_*.py` filenames remain
so existing command paths do not break.

## Before merging

1. Verify these repository **Actions secrets** exist:
   - `AIVEN_DATABASE_URL`: writer PostgreSQL URI, including password.
   - `AIVEN_CA_CERT`: the complete PEM CA certificate, with actual newlines.
   - `MIGRATION_SOURCE_DATABASE_URL`: Supabase **session** pooler URI on 5432.
   - Existing `API_BASE_URL`, Telegram, and dashboard-login secrets as applicable.
2. In **Settings → Secrets and variables → Actions → Variables**, create
   `AIVEN_CUTOVER_ENABLED` with value `false`.
3. In Streamlit Community Cloud → the app → Settings → Secrets, add Aiven
   settings **before merging**, retaining existing login settings. GitHub secrets
   are separate from Streamlit secrets. Example (replace placeholders):

   ```toml
   AIVEN_DATABASE_URL = "postgresql://USER:PASSWORD@HOST:PORT/defaultdb"
   AIVEN_CA_CERT = """
   -----BEGIN CERTIFICATE-----
   CERTIFICATE CONTENTS
   -----END CERTIFICATE-----
   """
   APP_USERNAME = "your existing username"
   APP_PASSWORD = "your existing password"
   ```

   Prefer a dedicated read-only dashboard user. Create the user in Aiven's Users
   page, then grant it the permissions below in Aiven SQL editor; replace
   `noise_dashboard` if you chose another username:

   ```sql
   GRANT CONNECT ON DATABASE defaultdb TO noise_dashboard;
   GRANT USAGE ON SCHEMA public TO noise_dashboard;
   GRANT SELECT ON public.wide_view_mv,
       public.monthly_persisted_summary TO noise_dashboard;
   CREATE POLICY aiven_dashboard_summary_read
       ON public.monthly_persisted_summary
       FOR SELECT TO noise_dashboard USING (true);
   ```

   Use that user's URI in **Streamlit**, and the writer URI in **GitHub Actions**.
   Existing view owners remain responsible for accessing their source tables.
4. Wait for the PostgreSQL test workflow on the pull request to pass, then merge.
   Streamlit redeploys from main. The application schedules are deliberately
   paused until the variable is changed to `true`; manual runs are available.

## Copy the recent Supabase readings

1. Go to **Actions → Aiven - Sync Recent Supabase Data → Run workflow**.
2. Select main; set `start_date` to `2026-10-20`.
3. Leave `end_date` blank to include today's Singapore date, or set `2026-11-02`
   to copy through 2 November only. Dates are inclusive Singapore calendar days.
4. Run and inspect **Copy, verify and refresh**.

The workflow reads one consistent Supabase snapshot, stages its recent readings,
and upserts by `(location_id, reading_datetime)` into Aiven. It preserves existing
Aiven IDs, numeric values, nulls, UTC timestamps and source created_at values.
It verifies every source row's fields against Aiven before committing. Extra Aiven
rows are reported and retained. An empty source window or a mismatch fails the
workflow rather than claiming success. A failed refresh after commit is safe to
retry: the copy is rerunnable.

It also copies current monthly summaries and preserves the exact stored contents
of the old `wide_view_mv_backup` as the ordinary archive table
`wide_view_mv_backup_snapshot`. Existing archives are retained. The backup
materialized view itself remains unpopulated; refreshing it would recompute and
change its historical snapshot. This archive consumes additional disk space.

The original gap checker only detected entirely empty days. Seeing readings for
each date is not a completeness guarantee. It also previously checked UTC dates
although its backfill fetched Singapore dates. The updated gap workflow uses
Singapore boundaries and offers **force**, defaulting to true, to re-fetch partial
days too. Check-only mode does not fetch or refresh.

## Verify API ingestion and enable schedules

1. Manually run **Hourly Noise Data ETL**. It writes directly from the raw API to
   Aiven and refreshes the dashboard view after successful ingestion.
2. Manually run **Daily Noise Data ETL** to exercise the previous-day path.
3. In Aiven SQL editor check the newest Singapore timestamp per location:

   ```sql
   SELECT location_id, count(*) AS readings,
          max(reading_datetime AT TIME ZONE 'Asia/Singapore') AS latest_sgt
   FROM public.meter_readings
   GROUP BY location_id ORDER BY location_id;
   ```

   Actual API downtime may leave some sensors behind. Zero readings and missing
   readings retain their existing meanings; they are not replaced or invented.
4. Test Streamlit login, date/location filters, yearly analysis, and CSV/Excel
   exports. Its data cache may take up to five minutes to expire; reboot if needed.
5. Run **Compute Monthly Noise Summary**. This preserves the existing behaviour
   of skipping previously computed past months. If a late backfill changes a past
   month, use the yearly tab's fresh calculation and review whether that month's
   stored summaries need recomputing before relying on them.
6. Run the weekly and monthly report workflows when ready. **Weekly Health Alert
   sends Telegram messages**, as before; do not trigger it merely as a silent test.
7. Set repository variable `AIVEN_CUTOVER_ENABLED` to `true`. The existing hourly,
   daily, weekly, and monthly schedules now target Aiven. No Supabase pg_cron job
   is needed on Aiven: refresh runs after successful ETL. Do not additionally
   schedule another overlapping refresh.

The monthly report and export use a direct PostgreSQL helper instead of the
Supabase REST API. The yearly tab consumes the same helper, preserving its filters
and metric calculations. Each query closes its connection so Streamlit reruns
do not accumulate connections on the small Aiven plan.

## Secrets and final retirement

Keep Supabase available until the data checks, dashboard, API ingestion and reports
are verified. The one-time source migration secret is not used by normal Aiven
jobs. Keep a recoverable database backup before retiring Supabase.

The previously committed `.env` and deployment examples contained credentials.
This change removes those committed values; history may still contain them.
Rotate any real exposed login/API secrets. Store replacement values exclusively
in GitHub, Streamlit or the deployment provider's secret settings.

The current Aiven trial must be converted to a funded ongoing service before it
expires. Check available disk space, performance and retention capacity before
retiring the source; the migration does not itself increase the selected plan.

## Tests

`python -m pytest tests/ -q` runs unit checks. With `NOISE_TEST_DATABASE_URL`
pointing to an **isolated test PostgreSQL instance whose user can create databases**,
it also runs integration checks in temporary databases. The PR workflow supplies
that service; it never reads production credentials or sends Telegram messages.
