-- create our data warehouse schemas: ODS for operational data copied from S3, staging for intermediate transformations
-- and DWH schema for reporting dedicated tables
create schema if not exists ODS;
create schema if not exists STAGING;
create schema if not exists DWH;

-- switch to DWH schema;
set search_path to DWH;
select current_schema();

-- create a dimension table for date
BEGIN TRANSACTION;

DROP TABLE IF EXISTS numbers_small;
CREATE TABLE numbers_small (
  number SMALLINT NOT NULL
) DISTSTYLE ALL SORTKEY (number
);
INSERT INTO numbers_small VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

DROP TABLE IF EXISTS numbers;
CREATE TABLE numbers (
  number BIGINT NOT NULL
) DISTSTYLE ALL SORTKEY (number
);
INSERT INTO numbers
  SELECT thousands.number * 1000 + hundreds.number * 100 + tens.number * 10 + ones.number
  FROM numbers_small thousands, numbers_small hundreds, numbers_small tens, numbers_small ones
  LIMIT 1000000;

DROP TABLE IF EXISTS "dim_date" CASCADE;
CREATE TABLE "dim_date" (
  "tk"                          INT4,
  "date"                        DATE,
  "day_of_week"                 FLOAT8,
  "day_of_week_name"            VARCHAR(9),
  "day_of_month"                INT4,
  "day_of_month_name"           VARCHAR(4),
  "day_of_year"                 INT4,
  "day_of_year_name"            VARCHAR(5),
  "week"                        INT4,
  "iso_week"                    INT4,
  "full_week"                   INT4,
  "week_name"                   VARCHAR(4),
  "week_end_date"               TIMESTAMP NULL,
  "week_start_date"             TIMESTAMP NULL,
  "month"                       INT4,
  "month_name"                  VARCHAR(9),
  "month_end_date"              TIMESTAMP NULL,
  "month_start_date"            TIMESTAMP NULL,
  "quarter"                     INT4,
  "quarter_name"                VARCHAR(2),
  "half_year"                   INT4,
  "half_year_name"              VARCHAR(2),
  "year"                        INT4,
  "year_end_date"               TIMESTAMP NULL,
  "year_start_date"             TIMESTAMP NULL,
  "is_weekday"                  boolean,
  "is_weekend"                  boolean,
  "us_holiday_identifier"       VARCHAR(30),
  "is_business_day"             boolean
) DISTSTYLE ALL SORTKEY (date);

INSERT INTO dim_date
(TK
  , "date"
  , day_of_week
  , day_of_week_name
  , day_of_month
  , day_of_month_name
  , day_of_year
  , day_of_year_name
  , week
  , week_name
  , week_end_date
  , week_start_date
  , "month"
  , month_name
  , month_end_date
  , month_start_date
  , quarter
  , quarter_name
  , half_year
  , half_year_name
  , "year"
  , year_end_date
  , year_start_date
  , is_weekday
  , is_weekend
)
  SELECT
    bas.TK,
    bas.date,
    bas.day_of_week,
    CASE bas.day_of_week
    WHEN 1
      THEN 'Sunday'
    WHEN 2
      THEN 'Monday'
    WHEN 3
      THEN 'Tuesday'
    WHEN 4
      THEN 'Wednesday'
    WHEN 5
      THEN 'Thursday'
    WHEN 6
      THEN 'Friday'
    WHEN 7
      THEN 'Saturday'
    END                                                               AS day_of_week_name,
    bas.day_of_month,
    CONVERT(VARCHAR(2), bas.day_of_month)
    + CASE RIGHT(CONVERT(VARCHAR(2), bas.day_of_month), 1)
      WHEN 1
        THEN CASE WHEN CONVERT(VARCHAR(2), bas.day_of_month) = '11'
          THEN 'th'
             ELSE 'st' END
      WHEN 2
        THEN CASE WHEN CONVERT(VARCHAR(2), bas.day_of_month) = '12'
          THEN 'th'
             ELSE 'nd' END
      WHEN 3
        THEN CASE WHEN CONVERT(VARCHAR(2), bas.day_of_month) = '13'
          THEN 'th'
             ELSE 'rd' END
      WHEN 4
        THEN 'th'
      WHEN 5
        THEN 'th'
      WHEN 6
        THEN 'th'
      WHEN 7
        THEN 'th'
      WHEN 8
        THEN 'th'
      WHEN 9
        THEN 'th'
      WHEN 0
        THEN 'th' END                                                 AS Day_of_month_name,                           
    bas.month,
    CASE bas.month
    WHEN 1
      THEN 'January'
    WHEN 2
      THEN 'February'
    WHEN 3
      THEN 'March'
    WHEN 4
      THEN 'April'
    WHEN 5
      THEN 'May'
    WHEN 6
      THEN 'June'
    WHEN 7
      THEN 'July'
    WHEN 8
      THEN 'August'
    WHEN 9
      THEN 'September'
    WHEN 10
      THEN 'October'
    WHEN 11
      THEN 'November'
    WHEN 12
      THEN 'December'
    END                                                               AS month_name,
  FROM (SELECT
          CONVERT(INT, TO_CHAR(DATEADD(day, num.number, '2010-01-01'), 'YYYYMMDD')) AS tk,
          CAST(DATEADD(day, num.number, '2010-01-01') AS DATE)                      AS "date",
          DATE_PART(dow, DATEADD(day, num.number, '2010-01-01')) + 1                AS day_of_week,
          DATEPART(day, DATEADD(day, num.number, '2010-01-01'))                     AS day_of_month,
          DATEPART(doy, DATEADD(day, num.number, '2010-01-01'))                     AS day_of_year,
          DATEPART(week, DATEADD(day, num.number, '2010-01-01'))                    AS week,
          DATEPART(month, DATEADD(day, num.number, '2010-01-01'))                   AS "month",
          DATEPART(quarter, DATEADD(day, num.number, '2010-01-01'))                 AS quarter,
          CASE WHEN DATEPART(qtr, DATEADD(day, num.number, '2010-01-01')) < 3
            THEN 1
          ELSE 2 END                                                                AS half_year,
          DATEPART(year, DATEADD(day, num.number, '2010-01-01'))                    AS "year",
          CASE WHEN DATEPART(dow, DATEADD(day, num.number, '2010-01-01')) IN (0, 6)
            THEN 0
          ELSE 1 END                                                                AS is_weekday,
          CASE WHEN DATEPART(dow, DATEADD(day, num.number, '2010-01-01')) IN (0, 6)
            THEN 1
          ELSE 0 END                                                                AS is_weekend
        FROM (SELECT *
              FROM numbers num
              LIMIT 5000) num
       ) bas;


DROP TABLE IF EXISTS tt_month_rank;
CREATE TEMP TABLE tt_month_rank AS
  SELECT
    dim_date.date,
    ROW_NUMBER()
    OVER (
      PARTITION BY year, month, day_of_week_name
      ORDER BY date )      AS month_day_name_rank,
    ROW_NUMBER()
    OVER (
      PARTITION BY year, month, day_of_week_name
      ORDER BY date DESC ) AS month_day_name_reverse_rank
  FROM dim_date;



DROP TABLE IF EXISTS tt_full_weeks_per_year;
CREATE TEMPORARY TABLE tt_full_weeks_per_year AS
  WITH days_per_week_per_year AS (
      SELECT
        "year",
        "week",
        count(1) days
      FROM dim_date
      WHERE "date" BETWEEN '2000-01-01' AND '3000-01-01'
      GROUP BY "year", "week"
  )
  SELECT
    "year",
    "week",
    ROW_NUMBER()
    OVER (
      PARTITION BY "year"
      ORDER BY "week" ) full_week
  FROM days_per_week_per_year
  WHERE days = 7;

UPDATE dim_date
SET full_week = tt_full_weeks_per_year.full_week
FROM tt_full_weeks_per_year
WHERE tt_full_weeks_per_year."year" = dim_date."year" AND tt_full_weeks_per_year.week = dim_date.week;



COMMIT TRANSACTION;

-- Switch back to ODS schema
set search_path to ODS;
select current_schema();

-- Pages data: COPY data from S3 parquet file to Redshift
-- create table definition
CREATE TABLE IF NOT EXISTS GA_PAGES_DATA(
  year bigint,
  month bigint,
  day bigint,
  hour bigint,
  minute bigint,
  page_title varchar,
  hostname varchar,
  page_path varchar,
  page_views bigint,
  unique_page_views bigint,
  time_on_page double precision,
  exits bigint,
  exit_rate double precision,
  entrances bigint);

-- load parquet data into table
copy GA_PAGES_DATA
from 's3://tayara-data-transformation/ga_transformed_data/ga_pages/20210523193023.parquet'
iam_role 'arn:aws:iam::237624289308:role/S3AccessRedshift'
format as parquet;

-- check loaded data
select * from ods.GA_PAGES_DATA limit 10;

-- Sessions Data: COPY data from S3 parquet file to Redshift
-- create table definition
CREATE TABLE IF NOT EXISTS GA_USERS_DATA(
  year bigint,
  month bigint,
  day bigint,
  hour bigint,
  device varchar,
  user_count bigint,
  new_users bigint,
  sessions bigint,
  sessions_per_user double precision,
  page_views bigint);

-- load parquet data into table
copy GA_USERS_DATA
from 's3://tayara-data-transformation/user/20210324160118.parquet'
iam_role 'arn:aws:iam::237624289308:role/S3AccessRedshift'
format as parquet;

-- check loaded data
select * from ods.GA_USERS_DATA limit 10;

-- Sessions Data: COPY data from S3 parquet file to Redshift
-- create table definition
CREATE TABLE IF NOT EXISTS GA_SESSIONS_DATA(
  year bigint,
  month bigint,
  day bigint,
  event_category varchar,
  browser varchar,
  location varchar,
  language varchar,
  user_count bigint,
  total_events bigint,
  unique_events bigint);

-- load parquet data into table
copy GA_SESSIONS_DATA
from 's3://tayara-data-transformation/ga-data/20210406105053.parquet'
iam_role 'arn:aws:iam::237624289308:role/S3AccessRedshift'
format as parquet;

-- check loaded data
select * from ods.GA_SESSIONS_DATA limit 10;