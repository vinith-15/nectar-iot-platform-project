-- 1. Top 10 assets by energy consumption
SELECT asset_id, SUM(energy_kwh) as total_energy
FROM fact_energy
GROUP BY asset_id
ORDER BY total_energy DESC
LIMIT 10;

-- 2. Average daily energy per site
SELECT site_id, AVG(daily_total) as avg_daily_energy
FROM (
    SELECT site_id, date, SUM(energy_kwh) as daily_total
    FROM fact_energy
    GROUP BY site_id, date
) site_daily
GROUP BY site_id;

-- 3. Assets with >10 faults in last 30 days
SELECT asset_id, COUNT(*) as fault_count
FROM fact_event
WHERE event_type = 'Fault' AND ingestion_timestamp >= NOW() - INTERVAL '30 days'
GROUP BY asset_id
HAVING COUNT(*) > 10;

-- 4. Assets with no telemetry in last 24 hours
SELECT DISTINCT asset_id
FROM dim_asset
WHERE asset_id NOT IN (
    SELECT DISTINCT asset_id
    FROM fact_telemetry
    WHERE ingestion_timestamp >= NOW() - INTERVAL '24 hours'
);

-- 5. Hourly utilization per building
SELECT building_id, DATE_TRUNC('hour', ingestion_timestamp) as hour,
       COUNT(*) as reading_count
FROM fact_telemetry
GROUP BY building_id, hour
ORDER BY building_id, hour;

-- 6. Sites with abnormal power increase
WITH daily_energy AS (
    SELECT site_id, date, SUM(energy_kwh) as total
    FROM fact_energy
    GROUP BY site_id, date
),
prev AS (
    SELECT site_id, date, total,
           LAG(total, 1) OVER (PARTITION BY site_id ORDER BY date) as prev_total
    FROM daily_energy
)
SELECT site_id, date, total, prev_total,
       ((total - prev_total) / prev_total) * 100 as pct_increase
FROM prev
WHERE prev_total IS NOT NULL
  AND (total - prev_total) / prev_total > 0.2
ORDER BY site_id, date;
