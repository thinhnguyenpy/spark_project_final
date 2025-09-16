
CREATE OR REPLACE PROCEDURE POPULATE_DW_SUMMARY_TABLES IS
    v_table_exists NUMBER;

    PROCEDURE drop_table_if_exists (p_table_name IN VARCHAR2) IS
    BEGIN
        SELECT COUNT(*)
        INTO v_table_exists
        FROM user_tables
        WHERE table_name = UPPER(p_table_name);

        IF v_table_exists > 0 THEN
            EXECUTE IMMEDIATE 'DROP TABLE ' || p_table_name;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Lỗi khi xóa bảng ' || p_table_name || ': ' || SQLERRM);
    END;

BEGIN
    drop_table_if_exists('DW_ACTIVE_DAYS_SUMMARY');
    EXECUTE IMMEDIATE '
    CREATE TABLE DW_ACTIVE_DAYS_SUMMARY AS
    SELECT "active_days",
           COUNT("userID") AS USER_COUNT
    FROM DW_ACTIVE_DAYS
    GROUP BY "active_days"
    ORDER BY "active_days"';

    drop_table_if_exists('DW_EVENTS_BY_HOUR');
    EXECUTE IMMEDIATE q'[
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
    ORDER BY "hour"]';

    drop_table_if_exists('DW_USAGE_BY_HOUR');
    EXECUTE IMMEDIATE '
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
    ON e."hour" = u."hour"';

    drop_table_if_exists('DW_EVENTS_BY_WEEKDAY');
    EXECUTE IMMEDIATE q'[
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
    ORDER BY "weekday"]';

    drop_table_if_exists('DW_USAGE_BY_WEEKDAY');
    EXECUTE IMMEDIATE '
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
    ON e."weekday" = u."weekday"';

    drop_table_if_exists('DW_USER_COUNT_BY_MONTH');
    EXECUTE IMMEDIATE '
    CREATE TABLE DW_USER_COUNT_BY_MONTH AS
    SELECT
        EXTRACT(YEAR FROM "SessionTimeStamp")  AS year,
        EXTRACT(MONTH FROM "SessionTimeStamp") AS month,
        COUNT(DISTINCT "userID") AS user_count
    FROM DW_BASE_SVC
    GROUP BY
        EXTRACT(YEAR FROM "SessionTimeStamp"),
        EXTRACT(MONTH FROM "SessionTimeStamp")
    ORDER BY year, month';

    drop_table_if_exists('DW_TOP_ITEMS_BY_PLAYTIME');
    EXECUTE IMMEDIATE '
    CREATE TABLE DW_TOP_ITEMS_BY_PLAYTIME AS
    SELECT
        "ItemID",
        SUM("RealTimePlaying") AS total_playing
    FROM DW_BASE_SVC
    WHERE "ItemID" IS NOT NULL
    GROUP BY "ItemID"
    ORDER BY SUM("RealTimePlaying") DESC
    FETCH FIRST 10 ROWS ONLY';

    drop_table_if_exists('DW_TOP_ITEMS_BY_USERID');
    EXECUTE IMMEDIATE '
    CREATE TABLE DW_TOP_ITEMS_BY_USERID AS
    SELECT
        "ItemID",
        COUNT(DISTINCT "userID") AS total_user
    FROM DW_BASE_SVC
    WHERE "ItemID" IS NOT NULL
    GROUP BY "ItemID"
    ORDER BY COUNT(DISTINCT "userID") DESC
    FETCH FIRST 10 ROWS ONLY';

    drop_table_if_exists('DW_TOTAL_EVENTS_BY_SERVICE');
    EXECUTE IMMEDIATE '
    CREATE TABLE DW_TOTAL_EVENTS_BY_SERVICE AS
    SELECT
        SUM("VOD") AS total_vod_events,
        SUM("IPTV") AS total_iptv_events,
        SUM("Timeshift") AS total_ts_events,
        SUM("System") AS total_system_events,
        SUM("Other") AS total_other_events
    FROM DW_USER_SERVICE_CNT';

    drop_table_if_exists('DW_AVG_RATIO_BY_SERVICE');
    EXECUTE IMMEDIATE '
    CREATE TABLE DW_AVG_RATIO_BY_SERVICE AS
    SELECT
     ROUND(AVG("VOD_ratio"), 3) AS avg_vod_ratio,
     ROUND(AVG("IPTV_ratio"), 3) AS avg_iptv_ratio,
     ROUND(AVG("TS_ratio"), 3) AS avg_ts_ratio
    FROM DW_USER_SERVICE_CNT';

    drop_table_if_exists('DW_VOD_CONV');
    EXECUTE IMMEDIATE '
    CREATE TABLE DW_VOD_CONV AS
    SELECT
        SUM(CASE WHEN "enter_vod" > 0 THEN 1 ELSE 0 END) AS users_enter,
        SUM(CASE WHEN "start_vod" > 0 THEN 1 ELSE 0 END) AS users_start,
        SUM(CASE WHEN "play_vod" > 0 THEN 1 ELSE 0 END)  AS users_play,
        SUM(CASE WHEN "stop_vod" > 0 THEN 1 ELSE 0 END)  AS users_stop,
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
    FROM DW_VOD_FUNNEL_USER';

    drop_table_if_exists('DW_IPTV_CONV');
    EXECUTE IMMEDIATE '
    CREATE TABLE DW_IPTV_CONV AS
    SELECT
        SUM(CASE WHEN "enter_iptv" > 0 THEN 1 ELSE 0 END) AS users_enter,
        SUM(CASE WHEN "start_ch" > 0 THEN 1 ELSE 0 END)   AS users_start,
        SUM(CASE WHEN "stop_ch" > 0 THEN 1 ELSE 0 END)    AS users_stop,
        ROUND(
          (SUM(CASE WHEN "start_ch" > 0 THEN 1 ELSE 0 END) * 1.0) /
          NULLIF(SUM(CASE WHEN "enter_iptv" > 0 THEN 1 ELSE 0 END),0), 4
        ) AS enter_to_start,
        ROUND(
          (SUM(CASE WHEN "stop_ch" > 0 THEN 1 ELSE 0 END) * 1.0) /
          NULLIF(SUM(CASE WHEN "start_ch" > 0 THEN 1 ELSE 0 END),0), 4
        ) AS start_to_stop
    FROM DW_IPTV_FUNNEL_USER';

    drop_table_if_exists('DW_USER_SEGMENT_SUMMARY');
    EXECUTE IMMEDIATE '
    CREATE TABLE DW_USER_SEGMENT_SUMMARY AS
    SELECT "segment", COUNT(*) AS user_count
    FROM (
        SELECT "userID", "segment" FROM DW_FEATURES
        UNION ALL
        SELECT "userID", "segment" FROM DW_INACTIVE_HARD
    )
    GROUP BY "segment"
    ORDER BY user_count DESC';

    DBMS_OUTPUT.PUT_LINE('Quy trình ETL đã hoàn thành thành công.');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Đã xảy ra lỗi trong quá trình ETL: ' || SQLERRM);
        RAISE; 
END POPULATE_DW_SUMMARY_TABLES;
/

-- run job tren

BEGIN
    BEGIN
        DBMS_SCHEDULER.DROP_JOB('JOB_POPULATE_DW_SUMMARY', force => TRUE);
    EXCEPTION
        WHEN OTHERS THEN
            NULL; 
    END;

    DBMS_SCHEDULER.CREATE_JOB (
        job_name        => 'JOB_POPULATE_DW_SUMMARY',
        job_type        => 'STORED_PROCEDURE',
        job_action      => 'POPULATE_DW_SUMMARY_TABLES',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=3; BYMINUTE=0;', 
        enabled         => TRUE,
        comments        => 'Job chạy hàng ngày để cập nhật các bảng summary trong Data Warehouse.'
    );
END;
/