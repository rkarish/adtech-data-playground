-- Flink SQL DDL: Register catalogs, create Iceberg tables, and source tables

-- 1. Register Iceberg REST catalog
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    'warehouse' = 's3://warehouse/'
);

CREATE DATABASE IF NOT EXISTS iceberg_catalog.db;

-- 2. Kafka source table for bid_requests (Avro via Schema Registry)
CREATE TEMPORARY TABLE kafka_bid_requests (
    `id` STRING,
    `imp` ARRAY<ROW<
        `id` STRING,
        `banner` ROW<`w` INT, `h` INT, `pos` INT>,
        `bidfloor` DOUBLE,
        `bidfloorcur` STRING,
        `secure` INT
    >>,
    `site` ROW<
        `id` STRING,
        `domain` STRING,
        `cat` ARRAY<STRING>,
        `page` STRING,
        `publisher` ROW<`id` STRING, `name` STRING>
    >,
    `app` ROW<
        `id` STRING,
        `bundle` STRING,
        `storeurl` STRING,
        `cat` ARRAY<STRING>,
        `publisher` ROW<`id` STRING, `name` STRING>
    >,
    `device` ROW<
        `ua` STRING,
        `ip` STRING,
        `geo` ROW<
            `lat` DOUBLE,
            `lon` DOUBLE,
            `country` STRING,
            `region` STRING
        >,
        `devicetype` INT,
        `os` STRING,
        `osv` STRING
    >,
    `user` ROW<
        `id` STRING,
        `buyeruid` STRING
    >,
    `at` INT,
    `tmax` INT,
    `cur` ARRAY<STRING>,
    `source` ROW<
        `fd` INT,
        `tid` STRING
    >,
    `regs` ROW<
        `coppa` INT,
        `ext` ROW<`gdpr` INT>
    >,
    `event_timestamp` STRING,
    `received_at` STRING,
    -- Computed column: parse ISO timestamp string to TIMESTAMP(3)
    `event_ts` AS TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    -- Watermark for event-time windowing (30 second tolerance for late data)
    WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bid-requests',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-bid-requests',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- 3. Kafka source table for bid_responses (Avro via Schema Registry)
CREATE TEMPORARY TABLE kafka_bid_responses (
    `id` STRING,
    `seatbid` ARRAY<ROW<
        `seat` STRING,
        `bid` ARRAY<ROW<
            `id` STRING,
            `impid` STRING,
            `price` DOUBLE,
            `adid` STRING,
            `crid` STRING,
            `adomain` ARRAY<STRING>,
            `dealid` STRING,
            `w` INT,
            `h` INT
        >>
    >>,
    `bidid` STRING,
    `cur` STRING,
    `ext` ROW<`request_id` STRING>,
    `event_timestamp` STRING,
    -- Computed column: parse ISO timestamp string to TIMESTAMP(3)
    `event_ts` AS TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    -- Watermark for event-time windowing (30 second tolerance for late data)
    WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bid-responses',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-bid-responses',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- 4. Kafka source table for impressions (Avro via Schema Registry)
CREATE TEMPORARY TABLE kafka_impressions (
    `impression_id` STRING,
    `request_id` STRING,
    `response_id` STRING,
    `imp_id` STRING,
    `bidder_id` STRING,
    `win_price` DOUBLE,
    `win_currency` STRING,
    `creative_id` STRING,
    `ad_domain` STRING,
    `event_timestamp` STRING,
    -- Computed column: parse ISO timestamp string to TIMESTAMP(3)
    `event_ts` AS TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    -- Watermark for event-time windowing (30 second tolerance for late data)
    WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'impressions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-impressions',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- 5. Kafka source table for clicks (Avro via Schema Registry)
CREATE TEMPORARY TABLE kafka_clicks (
    `click_id` STRING,
    `request_id` STRING,
    `impression_id` STRING,
    `imp_id` STRING,
    `bidder_id` STRING,
    `creative_id` STRING,
    `click_url` STRING,
    `event_timestamp` STRING,
    -- Computed column: parse ISO timestamp string to TIMESTAMP(3)
    `event_ts` AS TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    -- Watermark for event-time windowing (30 second tolerance for late data)
    WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-clicks',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- 6. Iceberg append-only tables (created via catalog DDL)

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.bid_requests (
    `request_id` STRING,
    `imp_id` STRING,
    `imp_banner_w` INT,
    `imp_banner_h` INT,
    `imp_bidfloor` DOUBLE,
    `site_id` STRING,
    `site_domain` STRING,
    `site_cat` ARRAY<STRING>,
    `publisher_id` STRING,
    `device_type` INT,
    `device_os` STRING,
    `device_geo_country` STRING,
    `device_geo_region` STRING,
    `user_id` STRING,
    `auction_type` INT,
    `tmax` INT,
    `currency` STRING,
    `is_coppa` BOOLEAN,
    `is_gdpr` BOOLEAN,
    `event_timestamp` TIMESTAMP_LTZ(6),
    `received_at` TIMESTAMP_LTZ(6)
) PARTITIONED BY (days(`event_timestamp`), `device_geo_country`)
WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.bid_responses (
    `response_id` STRING,
    `request_id` STRING,
    `seat` STRING,
    `bid_id` STRING,
    `imp_id` STRING,
    `bid_price` DOUBLE,
    `creative_id` STRING,
    `deal_id` STRING,
    `ad_domain` STRING,
    `currency` STRING,
    `event_timestamp` TIMESTAMP_LTZ(6)
) PARTITIONED BY (days(`event_timestamp`))
WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.impressions (
    `impression_id` STRING,
    `request_id` STRING,
    `response_id` STRING,
    `imp_id` STRING,
    `bidder_id` STRING,
    `win_price` DOUBLE,
    `win_currency` STRING,
    `creative_id` STRING,
    `ad_domain` STRING,
    `event_timestamp` TIMESTAMP_LTZ(6)
) PARTITIONED BY (days(`event_timestamp`))
WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.clicks (
    `click_id` STRING,
    `request_id` STRING,
    `impression_id` STRING,
    `imp_id` STRING,
    `bidder_id` STRING,
    `creative_id` STRING,
    `click_url` STRING,
    `event_timestamp` TIMESTAMP_LTZ(6)
) PARTITIONED BY (days(`event_timestamp`))
WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.bid_requests_enriched (
    `request_id` STRING,
    `imp_id` STRING,
    `imp_banner_w` INT,
    `imp_banner_h` INT,
    `imp_bidfloor` DOUBLE,
    `imp_bidfloor_usd` DOUBLE,
    `imp_bidfloorcur` STRING,
    `site_id` STRING,
    `site_domain` STRING,
    `app_id` STRING,
    `app_bundle` STRING,
    `publisher_id` STRING,
    `device_type` INT,
    `device_os` STRING,
    `device_ip` STRING,
    `device_geo_country` STRING,
    `device_geo_region` STRING,
    `device_category` STRING,
    `user_id` STRING,
    `auction_type` INT,
    `currency` STRING,
    `is_coppa` BOOLEAN,
    `is_gdpr` BOOLEAN,
    `is_test_traffic` BOOLEAN,
    `is_private_ip` BOOLEAN,
    `event_timestamp` TIMESTAMP_LTZ(6),
    `received_at` TIMESTAMP_LTZ(6)
) PARTITIONED BY (days(`event_timestamp`), `device_category`)
WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.dq_rejected_events (
    `request_id` STRING,
    `imp_id` STRING,
    `publisher_id` STRING,
    `device_ip` STRING,
    `reject_reason` STRING,
    `event_timestamp` TIMESTAMP_LTZ(6)
) PARTITIONED BY (days(`event_timestamp`))
WITH ('format-version' = '2');

-- 7. Iceberg upsert tables (created via catalog DDL with PRIMARY KEY)
-- Flink's PRIMARY KEY on catalog tables sets identifier-field-ids automatically.

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.hourly_impressions_by_geo (
    `window_start` TIMESTAMP(3),
    `device_geo_country` STRING,
    `impression_count` BIGINT,
    `total_revenue` DOUBLE,
    `avg_win_price` DOUBLE,
    PRIMARY KEY (`window_start`, `device_geo_country`) NOT ENFORCED
) PARTITIONED BY (days(`window_start`))
WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.rolling_metrics_by_bidder (
    `window_start` TIMESTAMP(3),
    `window_end` TIMESTAMP(3),
    `bidder_id` STRING,
    `win_count` BIGINT,
    `revenue` DOUBLE,
    `avg_cpm` DOUBLE,
    PRIMARY KEY (`window_start`, `bidder_id`) NOT ENFORCED
) PARTITIONED BY (days(`window_start`))
WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.hourly_funnel_by_publisher (
    `window_start` TIMESTAMP(3),
    `publisher_id` STRING,
    `bid_requests` BIGINT,
    `bid_responses` BIGINT,
    `impressions` BIGINT,
    `clicks` BIGINT,
    `fill_rate` DOUBLE,
    `win_rate` DOUBLE,
    `ctr` DOUBLE,
    PRIMARY KEY (`window_start`, `publisher_id`) NOT ENFORCED
) PARTITIONED BY (days(`window_start`))
WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.dq_event_quality_hourly (
    `window_start` TIMESTAMP(3),
    `total_bid_requests` BIGINT,
    `unique_bid_requests` BIGINT,
    `duplicate_bid_requests` BIGINT,
    `duplicate_bid_request_rate` DOUBLE,
    `total_bid_responses` BIGINT,
    `unique_bid_responses` BIGINT,
    `duplicate_bid_responses` BIGINT,
    `duplicate_bid_response_rate` DOUBLE,
    `total_wins` BIGINT,
    `unique_wins` BIGINT,
    `duplicate_wins` BIGINT,
    `duplicate_win_rate` DOUBLE,
    `total_clicks` BIGINT,
    `unique_clicks` BIGINT,
    `duplicate_clicks` BIGINT,
    `duplicate_click_rate` DOUBLE,
    `invalid_bid_requests` BIGINT,
    `invalid_bid_request_rate` DOUBLE,
    `total_events_all` BIGINT,
    `duplicate_events_all` BIGINT,
    `duplicate_rate_all` DOUBLE,
    PRIMARY KEY (`window_start`) NOT ENFORCED
) PARTITIONED BY (days(`window_start`))
WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.bid_landscape_hourly (
    `window_start` TIMESTAMP(3),
    `publisher_id` STRING,
    `request_count` BIGINT,
    `total_bids` BIGINT,
    `bids_per_request` DOUBLE,
    `avg_bid_price` DOUBLE,
    `max_bid_price` DOUBLE,
    PRIMARY KEY (`window_start`, `publisher_id`) NOT ENFORCED
) PARTITIONED BY (days(`window_start`))
WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.realtime_serving_metrics_1m (
    `window_start` TIMESTAMP(3),
    `bidder_id` STRING,
    `impressions` BIGINT,
    `clicks` BIGINT,
    `revenue` DOUBLE,
    `ctr` DOUBLE,
    PRIMARY KEY (`window_start`, `bidder_id`) NOT ENFORCED
) PARTITIONED BY (days(`window_start`))
WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS iceberg_catalog.db.funnel_leakage_hourly (
    `window_start` TIMESTAMP(3),
    `publisher_id` STRING,
    `requests_no_response` BIGINT,
    `responses_no_impression` BIGINT,
    `impressions_no_click` BIGINT,
    `response_leakage_rate` DOUBLE,
    `impression_leakage_rate` DOUBLE,
    `click_leakage_rate` DOUBLE,
    PRIMARY KEY (`window_start`, `publisher_id`) NOT ENFORCED
) PARTITIONED BY (days(`window_start`))
WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');
