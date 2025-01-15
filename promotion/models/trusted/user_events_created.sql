MODEL (
  name trusted.user_events_created,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_timestamp
  ),
  start '2020-01-01',
  cron '@daily'
);

SELECT
    event_uuid,
    event_timestamp,
    account_id,
    balance,
    status,
    user_id
FROM
    main.raw_user_events_created
-- WHERE
--     event_timestamp BETWEEN @start_date AND @end_date
