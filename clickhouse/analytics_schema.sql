-- =====================================================
-- 0. Drop and recreate the database
-- =====================================================

CREATE DATABASE IF NOT EXISTS analytics;

-- =====================================================
-- 1. Base tables (non‑replicated, dedup ready)
-- =====================================================

-- Operations (no source_id in actual table)
CREATE TABLE IF NOT EXISTS analytics.operations
(
    service   LowCardinality(String),
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    branch_id LowCardinality(String),
    gender    LowCardinality(String),
    age_group LowCardinality(String),
    action    LowCardinality(String),
    event_id  String DEFAULT generateUUIDv4(),   -- dedup key
    INDEX idx_branch branch_id TYPE set(0) GRANULARITY 1
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (branch_id, timestamp, event_id)
SETTINGS index_granularity = 8192;

-- Business (has source_id)
CREATE TABLE IF NOT EXISTS analytics.business
(
    service   LowCardinality(String),
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    tin       Nullable(DateTime64(3, 'UTC')) CODEC(DoubleDelta, ZSTD(1)),
    tout      Nullable(DateTime64(3, 'UTC')) CODEC(DoubleDelta, ZSTD(1)),
    seconds   Int32,
    roi_name  LowCardinality(String),
    source_id Int32,
    branch_id LowCardinality(String),
    event_id  String DEFAULT generateUUIDv4(),   -- dedup key
    INDEX idx_branch branch_id TYPE set(0) GRANULARITY 1
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (branch_id, source_id, timestamp, event_id)
SETTINGS index_granularity = 8192;

-- Alarms (source_id is String, alarm_id is String)
CREATE TABLE IF NOT EXISTS analytics.alarms
(
    service    LowCardinality(String),
    roi_name   LowCardinality(String),
    tin        Nullable(DateTime64(3, 'UTC')) CODEC(DoubleDelta, ZSTD(1)),
    seconds    Int32,
    timestamp  DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    video_path String,
    alarm_id   String DEFAULT generateUUIDv4(),   -- dedup key (UUID as String)
    branch_id  LowCardinality(String),
    source_id  String,
    INDEX idx_branch branch_id TYPE set(0) GRANULARITY 1,
    INDEX idx_service service TYPE set(0) GRANULARITY 1
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (branch_id, source_id, timestamp, alarm_id)
SETTINGS index_granularity = 8192;

-- Anomaly detections (no dedup needed)
CREATE TABLE IF NOT EXISTS analytics.anomaly_detections
(
    detected_at   DateTime,
    branch_id     String,
    metric        String,
    service       String,
    event_date    Date,
    value         Float64,
    anomaly_score Float64,
    expected_low  Float64 DEFAULT 0,
    expected_high Float64 DEFAULT 0,
    window_start  Date,
    window_end    Date,
    roi_name      String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY (branch_id, service, metric, detected_at)
TTL detected_at + toIntervalDay(30)
SETTINGS index_granularity = 8192;

-- =====================================================
-- 2. Aggregated tables (SummingMergeTree)
-- =====================================================

CREATE TABLE IF NOT EXISTS analytics.operations_branch_daily_stats
(
    event_date    Date,
    branch_id     String,
    visitors      UInt64,
    males         UInt64,
    females       UInt64,
    age_20_30     UInt64,
    age_30_40     UInt64,
    age_40_50     UInt64,
    age_50_60     UInt64,
    age_60_plus   UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, branch_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS analytics.served_customers_hourly
(
    event_date    Date,
    event_hour    UInt8,
    branch_id     LowCardinality(String),
    employee_name LowCardinality(String),
    customer_type Enum8('other' = 0, 'cs' = 1, 'axa' = 2, 'teller' = 3, 'atm' = 4),
    served_count  UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, branch_id, customer_type, employee_name, event_hour)
SETTINGS index_granularity = 8192;

-- =====================================================
-- 3. Materialized Views (populate aggregated tables)
-- =====================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_operations_branch_daily_stats
TO analytics.operations_branch_daily_stats
(
    event_date    Date,
    branch_id     LowCardinality(String),
    visitors      UInt64,
    males         Int32,
    females       Int32,
    age_20_30     Int32,
    age_30_40     Int32,
    age_40_50     Int32,
    age_50_60     Int32,
    age_60_plus   Int32
)
AS SELECT
    toDate(ts_cairo) AS event_date,
    branch_id,
    countIf(toHour(ts_cairo) < if((toDate(ts_cairo) >= '2026-02-22') AND (toDate(ts_cairo) <= '2026-03-20'), 16, 15)) AS visitors,
    toInt32(if(countIf(gender IN ('male', 'female')) > 0, round(count(*) * (countIf(gender = 'male') / countIf(gender IN ('male', 'female', ' ')))), 0)) AS males,
    toInt32(count(*) - if(countIf(gender IN ('male', 'female')) > 0, round(count(*) * (countIf(gender = 'male') / countIf(gender IN ('male', 'female')))), 0)) AS females,
    toInt32(if(count(age_mapped) > 0, count(*) * (countIf(age_mapped = '20-30') / count(age_mapped)), 0)) AS age_20_30,
    toInt32(if(count(age_mapped) > 0, count(*) * (countIf(age_mapped = '30-40') / count(age_mapped)), 0)) AS age_30_40,
    toInt32(if(count(age_mapped) > 0, count(*) * (countIf(age_mapped = '40-50') / count(age_mapped)), 0)) AS age_40_50,
    toInt32(if(count(age_mapped) > 0, count(*) * (countIf(age_mapped = '50-60') / count(age_mapped)), 0)) AS age_50_60,
    toInt32(if(count(age_mapped) > 0, count(*) * (countIf(age_mapped = '60+') / count(age_mapped)), 0)) AS age_60_plus
FROM
(
    SELECT
        branch_id,
        gender,
        toTimeZone(timestamp, 'Africa/Cairo') AS ts_cairo,
        multiIf(age_group = '16-30', '20-30', (age_group = '31-45') AND ((cityHash64(timestamp, branch_id) % 100) < 50), '30-40', age_group = '31-45', '40-50', (age_group = '46-60') AND ((cityHash64(timestamp, branch_id) % 100) < 50), '40-50', age_group = '46-60', '50-60', age_group = '61+', '60+', (age_group = 'unknown') AND ((cityHash64(timestamp, branch_id) % 100) < 60), '30-40', (age_group = 'unknown') AND ((cityHash64(timestamp, branch_id) % 100) < 90), '40-50', age_group = 'unknown', '50-60', NULL) AS age_mapped
    FROM analytics.operations
    WHERE (action = 'in') AND (toDayOfWeek(toTimeZone(timestamp, 'Africa/Cairo')) NOT IN (5, 6)) AND (toHour(toTimeZone(timestamp, 'Africa/Cairo')) >= 8) AND (toHour(toTimeZone(timestamp, 'Africa/Cairo')) < if((toDate(toTimeZone(timestamp, 'Africa/Cairo')) >= toDate('2026-02-22')) AND (toDate(toTimeZone(timestamp, 'Africa/Cairo')) <= toDate('2026-03-20')), 16, 17))
)
GROUP BY
    event_date,
    branch_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_served_customers
TO analytics.served_customers_hourly
(
    event_date    Date,
    event_hour    UInt8,
    branch_id     LowCardinality(String),
    employee_name LowCardinality(String),
    customer_type String,
    served_count  UInt64
)
AS WITH
    toTimeZone(timestamp, 'Africa/Cairo') AS ts_cairo,
    toDate(ts_cairo) AS d,
    toHour(ts_cairo) AS h,
    if((d >= toDate('2026-02-22')) AND (d <= toDate('2026-03-20')), 16, 17) AS end_hour,
    multiIf(roi_name LIKE 'ATM % SMT', 'atm', (lower(roi_name) LIKE '%customer service%') AND (roi_name NOT LIKE '%AXA%'), 'cs', lower(roi_name) LIKE '%axa%', 'axa', lower(roi_name) LIKE '%teller%', 'teller', 'other') AS customer_type
SELECT
    d AS event_date,
    h AS event_hour,
    branch_id,
    roi_name AS employee_name,
    customer_type,
    count() AS served_count
FROM analytics.business
WHERE (service = 'service-mean-time') AND (customer_type != 'other') AND (((customer_type IN ('cs', 'axa', 'teller')) AND (toDayOfWeek(ts_cairo) NOT IN (5, 6)) AND (h >= 8) AND (h < end_hour)) OR (customer_type = 'atm')) AND (((customer_type IN ('cs', 'axa')) AND (seconds >= 80) AND (seconds <= 300)) OR ((customer_type = 'teller') AND (seconds >= 80) AND (seconds <= 300)) OR ((customer_type = 'atm') AND (seconds >= 20) AND (seconds <= 60)))
GROUP BY
    event_date,
    event_hour,
    branch_id,
    employee_name,
    customer_type;
