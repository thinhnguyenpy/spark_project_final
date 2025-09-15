from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Thinh") \
    .config("spark.local.dir", "D:/spark-temp") \
    .config("spark.jars", "D:\jars\ojdbc11.jar") \
    .getOrCreate()

df = spark.read.parquet("./data_lake/df")
df1 = spark.read.parquet("./data_lake/df1")
df2 = spark.read.parquet("./data_lake/df2")

from pyspark.sql import functions as F

def usage_by_user(df):
    return df.groupBy("userID") \
             .agg(F.sum(F.coalesce(F.col("RealTimePlaying"), F.lit(0))).alias("total_playing_time"))

def usage_by_device(df):
    return df.groupBy("AppName") \
             .agg(F.sum(F.coalesce(F.col("RealTimePlaying"), F.lit(0))).alias("total_playing_time"),
                  F.count("Event").alias("num_events"))

def segment_users(df_user):
    return df_user.withColumn(
        "customer_segment", 
        F.when(F.col("days") < 30, "new_user")
         .when((F.col("days") >= 30) & (F.col("days") < 180), "mid_user")
         .otherwise("loyal_user")
    )

def user_segment_count(segmented):
    return segmented.groupBy("customer_segment") \
                    .agg(F.countDistinct("userID").alias("number_of_customer"))
r
def event_count(df):
    return df.groupBy("userID", "Event") \
             .agg(F.count("*").alias("event_count"))

def active_days(df):
    return df.withColumn("date", F.to_date("SessionTimeStamp")) \
             .groupBy("userID") \
             .agg(F.countDistinct("date").alias("active_days"))

def content_diversity(df):
    return df.groupBy("userID") \
             .agg(F.countDistinct("ItemID").alias("unique_items_watched"))


jdbc_url = "jdbc:oracle:thin:@localhost:1521/XEPDB1"
driver = "oracle.jdbc.driver.OracleDriver"
user = "dev"
password = "1"

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

def run_and_save_all(df, df_user, url, driver, user, password):
    segmented = segment_users(df_user)

    metrics = {
        "dw_usage_by_user": usage_by_user(df),
        "dw_usage_by_device": usage_by_device(df),
        "dw_user_segment_count": user_segment_count(segmented),
        "dw_event_count": event_count(df),
        "dw_active_days": active_days(df),
        "dw_content_diversity": content_diversity(df),
    }

    for table_name, result_df in metrics.items():
        save_to_dw(result_df, table_name, url, driver, user, password)