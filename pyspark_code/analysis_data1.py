from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder \
    .appName("Thinh") \
    .config("spark.local.dir", "D:/spark-temp") \
    .config("spark.jars", "D:\jars\ojdbc11.jar") \
    .getOrCreate()
    
df = spark.read.parquet('./data_lake/df')
user = spark.read.parquet('./data_lake/df1') 

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, sum, avg, lit, count, hour, month, coalesce,
    lead
)

def get_base_svc(df):
    vod_events = ["StartVOD", "PlayVOD", "StopVOD", "PauseVOD", "NextVOD", "PreviousVOD", 
              "EnterVOD", "EnterDetailVOD", "EnterFolderVOD", "EnterSearchVOD", 
              "SearchVOD", "ChangeModule", "RemoveFavorite", "InsertFavorite"]

    iptv_events = ["EnterIPTV", "StartChannel", "StopChannel", "ShowChannelList", "ExitChannelList", 
                "FavouriteChannelList", "ShowEPG", "ExitEPG", "Show schedule", "ExitSchedule",
                "PiPSwitch", "ExitPIP"]

    timeshift_events = ["StartTimeshift", "StopTimeshift"]

    system_events = ["Standby", "Turn on from Stanby", "SetAlarm", 
                  "GetDRMKeySuccessful", "GetDRMKeyFailed"]

    return (
        df.withColumn(
            "service",
            when(col("event").isin(vod_events), "VOD")
            .when(col("event").isin(iptv_events), "IPTV")
            .when(col("event").isin(timeshift_events), "Timeshift")
            .when(col("event").isin(system_events), "System")
            .otherwise("Other")
        )
        .withColumn("hour", hour(col("SessionTimeStamp")))
        .withColumn("weekday", weekday(col("SessionTimeStamp")))
        .withColumn("month", month(col("SessionTimeStamp")))
    )



def get_user_service_counts(base_svc):
    """Đếm số sự kiện từng loại dịch vụ theo user"""
    return (
        base_svc.groupBy("userID", "service").count()
        .groupBy("userID")
        .pivot("service", ["VOD","IPTV","Timeshift","System","Other"])
        .sum("count")
        .na.fill(0)
        .withColumn(
            "total_events",
            col("VOD") + col("IPTV") + col("Timeshift") + col("System") + col("Other")
        )
        .withColumn("VOD_ratio",  when(col("total_events")>0, col("VOD")/col("total_events")).otherwise(lit(0.0)))
        .withColumn("IPTV_ratio", when(col("total_events")>0, col("IPTV")/col("total_events")).otherwise(lit(0.0)))
        .withColumn("TS_ratio",   when(col("total_events")>0, col("Timeshift")/col("total_events")).otherwise(lit(0.0)))
    )


def get_funnel(df, service="VOD"):
    """Tính funnel cho VOD hoặc IPTV"""
    if service == "VOD":
        return (
            df.filter(col("service")=="VOD")
              .groupBy("userID")
              .agg(
                  sum(when(col("Event")=="EnterVOD",1).otherwise(0)).alias("enter_vod"),
                  sum(when(col("Event")=="StartVOD",1).otherwise(0)).alias("start_vod"),
                  sum(when(col("Event")=="PlayVOD",1).otherwise(0)).alias("play_vod"),
                  sum(when(col("Event")=="StopVOD",1).otherwise(0)).alias("stop_vod")
              )
        )
    else:
        return (
            df.filter(col("service")=="IPTV")
              .groupBy("userID")
              .agg(
                  sum(when(col("Event")=="EnterIPTV",1).otherwise(0)).alias("enter_iptv"),
                  sum(when(col("Event")=="StartChannel",1).otherwise(0)).alias("start_ch"),
                  sum(when(col("Event")=="StopChannel",1).otherwise(0)).alias("stop_ch")
              )
        )


def get_paired_sessions(base_svc):
    """Ghép event liên tiếp để tính thời lượng session"""
    w = Window.partitionBy("userID").orderBy("SessionTimeStamp")
    return (
        base_svc
        .withColumn("next_event", lead("Event").over(w))
        .withColumn("next_ts", lead("SessionTimeStamp").over(w))
        .withColumn(
            "vod_session_sec",
            when(col("Event").isin("PlayVOD","StartVOD") & (col("next_event")=="StopVOD"),
                 (col("next_ts").cast("long")-col("SessionTimeStamp").cast("long")).cast("double"))
        )
        .withColumn(
            "iptv_session_sec",
            when((col("Event")=="StartChannel") & (col("next_event")=="StopChannel"),
                 (col("next_ts").cast("long")-col("SessionTimeStamp").cast("long")).cast("double"))
        )
        .withColumn(
            "ts_session_sec",
            when((col("Event")=="StartTimeshift") & (col("next_event")=="StopTimeshift"),
                 (col("next_ts").cast("long")-col("SessionTimeStamp").cast("long")).cast("double"))
        )
        .withColumn("session_sec", coalesce("vod_session_sec","iptv_session_sec","ts_session_sec","RealTimePlaying"))
    )


def get_user_sessions(paired):
    """Tính tổng thời gian, số session, số zapping cho mỗi user"""
    return (
        paired.groupBy("userID")
        .agg(
            sum("session_sec").alias("total_watch_sec"),
            avg("session_sec").alias("avg_session_sec"),
            sum(when(col("session_sec").isNotNull(),1).otherwise(0)).alias("num_sessions"),
            sum(when((col("iptv_session_sec").isNotNull()) & (col("iptv_session_sec")<60),1).otherwise(0)).alias("zapping_sessions")
        )
    )


def get_explore_play(base_svc):
    """Đếm hành vi explore và play VOD"""
    return (
        base_svc.groupBy("userID")
        .agg(
            sum(when(col("Event").isin("EnterSearchVOD","EnterFolderVOD","SearchVOD","EnterDetailVOD"),1).otherwise(0)).alias("explore_events"),
            sum(when(col("Event")=="PlayVOD",1).otherwise(0)).alias("play_vod_cnt")
        )
    )


def build_features(user, user_service_counts, user_sessions, explore_play, paired):
    """Tích hợp tất cả feature lại"""
    iptv_sessions_cnt = (
        paired.select("userID", when(col("iptv_session_sec").isNotNull(),1).otherwise(0).alias("is_iptv_sess"))
        .groupBy("userID").agg(sum("is_iptv_sess").alias("iptv_sessions"))
    )

    features = (
        user_service_counts
        .join(user_sessions, on="userID", how="left")
        .join(user.select("userID","device_type","days"), on="userID", how="left")
        .na.fill({"total_watch_sec":0.0,"avg_session_sec":0.0,"num_sessions":0,"zapping_sessions":0})
        .join(explore_play, on="userID", how="left")
        .na.fill({"explore_events":0,"play_vod_cnt":0})
        .join(iptv_sessions_cnt, on="userID", how="left")
        .na.fill({"iptv_sessions":0})
        .withColumn("zapping_ratio", when(col("iptv_sessions")>0, col("zapping_sessions")/col("iptv_sessions")).otherwise(lit(0.0)))
        .withColumn(
            "segment",
            when((col("VOD_ratio")>=0.6) & (col("play_vod_cnt")>=10), lit("heavy_vod"))
            .when((col("IPTV_ratio")>=0.6) & (col("iptv_sessions")>=10), lit("heavy_iptv"))
            .when((col("iptv_sessions")>=10) & (col("zapping_ratio")>=0.7), lit("zapper"))
            .when((col("explore_events")>=5) & (col("play_vod_cnt")<=5), lit("explorer"))
            .when(col("total_events")<5, lit("inactive"))
            .otherwise(lit("mixed"))
        )
    )

    inactive_hard = (
        user.join(df.select("userID").distinct(), on="userID", how="left_anti")
            .withColumn("segment", lit("inactive_no_log"))
    )

    return features, inactive_hard

dw_url = "jdbc:oracle:thin:@localhost:1521/XEPDB1"
dw_driver = "oracle.jdbc.driver.OracleDriver"
dw_user = "dev"
dw_pass = "1"

def save_to_dw(df, table_name, url, driver, user, password, mode="overwrite"):
    try:
        (df.write.format("jdbc")
           .option("url", url)
           .option("driver", driver)
           .option("dbtable", table_name)
           .option("user", user)
           .option("password", password)
           .mode(mode)
           .save())
        print(f"Data saved to {table_name} successfully.")
    except Exception as e:
        import traceback
        print("Error saving to DW:")
        traceback.print_exc()
        print("="*50)
        print(str(e))
        
def run_full_etl_and_save(df, user, url, driver, user_db, password):
    
    base_svc = get_base_svc(df)
    user_service_counts = get_user_service_counts(base_svc)
    vod_funnel_user = get_funnel(base_svc, service="VOD")
    iptv_funnel_user = get_funnel(base_svc, service="IPTV")
    paired = get_paired_sessions(base_svc)
    user_sessions = get_user_sessions(paired)
    explore_play = get_explore_play(base_svc)
    features, inactive_hard = build_features(user, user_service_counts, user_sessions, explore_play, paired)

    print("da read xong cac df")
    dw_tables = {
        "DW_BASE_SVC": base_svc,
        "DW_USER_SERVICE_CNT": user_service_counts,
        "DW_VOD_FUNNEL_USER": vod_funnel_user,
        "DW_IPTV_FUNNEL_USER": iptv_funnel_user,
        "DW_PAIRED_LOG": paired,
        "DW_USER_SESSIONS": user_sessions,
        "DW_EXPLORE_PLAY": explore_play,
        "DW_FEATURES": features,
        "DW_INACTIVE_HARD": inactive_hard,
    }

    
    for table_name, df_table in dw_tables.items():
        save_to_dw(df_table, table_name, url, driver, user_db, password)

    print(" luu thanh cong tat ca cac df")

