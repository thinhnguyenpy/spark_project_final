
CREATE TABLE DW_ACTIVE_DAYS_SUMMARY AS 
SELECT "active_days",
       COUNT("userID") AS USER_COUNT
FROM DW_ACTIVE_DAYS
GROUP BY "active_days"
ORDER BY "active_days";

Select * From DW_ACTIVE_DAYS_SUMMARY;

SELECT 
    "hour",
    "service",
    COUNT(*) AS event_count
FROM DW_BASE_SVC
GROUP BY "hour", "service"
ORDER BY "hour", "service";


CREATE TABLE DW_EVENTS_BY_HOUR AS
SELECT *
FROM (
    SELECT "hour",
           "service",
           "userID"
    FROM DW_BASE_SVC
) 
PIVOT (
    COUNT("userID") 
    FOR "service" IN (
        'IPTV' AS IPTV,
        'System' AS SYSTEM,
        'Timeshift' AS TIMESHIFT,
        'VOD' AS VOD
    )
)
ORDER BY "hour";

CREATE TABLE DW_USAGE_BY_HOUR AS
SELECT 
    e."hour",
    e.IPTV,
    e.SYSTEM,
    e.TIMESHIFT,
    e.VOD,
    u.user_count
FROM DW_EVENTS_BY_HOUR e
JOIN (
    SELECT "hour",
           COUNT(DISTINCT "userID") AS user_count
    FROM DW_BASE_SVC
    GROUP BY "hour"
) u
ON e."hour" = u."hour";

Select * From DW_USAGE_BY_HOUR;

Select * From dw_base_svc;

CREATE TABLE DW_EVENTS_BY_WEEKDAY AS
SELECT *
FROM (
    SELECT "weekday",
           "service",
           "userID"
    FROM DW_BASE_SVC
) 
PIVOT (
    COUNT("userID") 
    FOR "service" IN (
        'IPTV' AS IPTV,
        'System' AS SYSTEM,
        'Timeshift' AS TIMESHIFT,
        'VOD' AS VOD
    )
)
ORDER BY "weekday";

CREATE TABLE DW_USAGE_BY_WEEKDAY AS
SELECT 
    e."weekday",
    e.IPTV,
    e.SYSTEM,
    e.TIMESHIFT,
    e.VOD,
    u.user_count
FROM DW_EVENTS_BY_WEEKDAY e
JOIN (
    SELECT "weekday",
           COUNT(DISTINCT "userID") AS user_count
    FROM DW_BASE_SVC
    GROUP BY "weekday"
) u
ON e."weekday" = u."weekday";

Select * From DW_USAGE_BY_WEEKDAY;

Select * From dw_base_svc;

CREATE TABLE DW_USER_COUNT_BY_MONTH AS
SELECT  
    EXTRACT(MONTH FROM "SessionTimeStamp") AS month,
    EXTRACT(YEAR FROM "SessionTimeStamp")  AS year,
    COUNT(DISTINCT "userID") AS user_count
FROM DW_BASE_SVC
GROUP BY 
    EXTRACT(MONTH FROM "SessionTimeStamp"),
    EXTRACT(YEAR FROM "SessionTimeStamp")
ORDER BY year, month;


CREATE TABLE DW_TOP_ITEMS_BY_PLAYTIME AS
SELECT 
    "ItemID", 
    SUM("RealTimePlaying") AS total_playing
FROM DW_BASE_SVC
GROUP BY "ItemID"
ORDER BY SUM("RealTimePlaying") DESC
FETCH FIRST 10 ROWS ONLY;

CREATE TABLE DW_TOP_ITEMS_BY_USERID AS
SELECT 
    "ItemID", 
    COUNT(DISTINCT "userID") AS total_user
FROM DW_BASE_SVC
Where "ItemID" IS NOT NULL
GROUP BY "ItemID"
ORDER BY COUNT(DISTINCT "userID") DESC
FETCH FIRST 10 ROWS ONLY;


CREATE TABLE DW_TOTAL_EVENTS_BY_SERVICE AS
SELECT 
    SUM("VOD") AS total_vod_events,
    SUM("IPTV") AS total_iptv_events,
    SUM("Timeshift") AS total_ts_events,
    SUM("System") AS total_system_events,
    SUM("Other") AS total_other_events
FROM DW_USER_SERVICE_CNT;


CREATE TABLE DW_AVG_RATIO_BY_SERVICE AS
SELECT 
  ROUND(AVG("VOD_ratio"), 3) AS avg_vod_ratio,
  ROUND(AVG("IPTV_ratio"), 3) AS avg_iptv_ratio,
  ROUND(AVG("TS_ratio"), 3) AS avg_ts_ratio
FROM DW_USER_SERVICE_CNT;

CREATE TABLE DW_VOD_CONV AS
SELECT 
    SUM(CASE WHEN "enter_vod" > 0 THEN 1 ELSE 0 END) AS users_enter,
    SUM(CASE WHEN "start_vod" > 0 THEN 1 ELSE 0 END) AS users_start,
    SUM(CASE WHEN "play_vod" > 0 THEN 1 ELSE 0 END)  AS users_play,
    SUM(CASE WHEN "stop_vod" > 0 THEN 1 ELSE 0 END)  AS users_stop,
    -- conversion ratios rounded 4 decimals
    ROUND(
      (SUM(CASE WHEN "start_vod" > 0 THEN 1 ELSE 0 END) * 1.0) /
      NULLIF(SUM(CASE WHEN "enter_vod" > 0 THEN 1 ELSE 0 END),0), 4
    ) AS enter_to_start,
    ROUND(
      (SUM(CASE WHEN "play_vod" > 0 THEN 1 ELSE 0 END) * 1.0) /
      NULLIF(SUM(CASE WHEN "start_vod" > 0 THEN 1 ELSE 0 END),0), 4
    ) AS start_to_play,
    ROUND(
      (SUM(CASE WHEN "stop_vod" > 0 THEN 1 ELSE 0 END) * 1.0) /
      NULLIF(SUM(CASE WHEN "play_vod" > 0 THEN 1 ELSE 0 END),0), 4
    ) AS play_to_stop
FROM DW_VOD_FUNNEL_USER;

Select * From DW_VOD_CONV;

CREATE TABLE DW_IPTV_CONV AS
SELECT 
    SUM(CASE WHEN "enter_iptv" > 0 THEN 1 ELSE 0 END) AS users_enter,
    SUM(CASE WHEN "start_ch" > 0 THEN 1 ELSE 0 END)   AS users_start,
    SUM(CASE WHEN "stop_ch" > 0 THEN 1 ELSE 0 END)    AS users_stop,
    -- conversion ratios rounded 4 decimals
    ROUND(
      (SUM(CASE WHEN "start_ch" > 0 THEN 1 ELSE 0 END) * 1.0) /
      NULLIF(SUM(CASE WHEN "enter_iptv" > 0 THEN 1 ELSE 0 END),0), 4
    ) AS enter_to_start,
    ROUND(
      (SUM(CASE WHEN "stop_ch" > 0 THEN 1 ELSE 0 END) * 1.0) /
      NULLIF(SUM(CASE WHEN "start_ch" > 0 THEN 1 ELSE 0 END),0), 4
    ) AS start_to_stop
FROM DW_IPTV_FUNNEL_USER;

Create Table DW_USER_SEGMENT_SUMMARY AS
SELECT "segment", COUNT(*) AS user_count
FROM (
    SELECT "userID", "segment" FROM DW_FEATURES
    UNION ALL
    SELECT "userID", "segment" FROM DW_INACTIVE_HARD
)
GROUP BY "segment"
ORDER BY user_count DESC;



