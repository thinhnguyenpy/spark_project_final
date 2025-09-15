from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

sc = SparkContext()
spark = SparkSession.builder \
  .appName("LogAnalysis")\
  .config("spark.local.dir", "D:/spark-temp") \
  .getOrCreate()
  
import os, ast, json 
folder_path = './DataSampleTest'
out_path = './data_lake'

def parse_line(line):
  try:
      py_dict = ast.literal_eval(line)
      return json.dumps(py_dict, ensure_ascii=True)
  
  except Exception as e:
    return None
  
from pyspark.sql.functions import try_to_timestamp

def convert_format(file_name):

    rdd = sc.textFile(file_name)
    json_rdd = rdd.map(parse_line)
    df = spark.read.json(json_rdd)

    df = df.withColumn(
        "SessionTime",
        concat_ws(' ',
            concat_ws('-',
                split(col("SessionMainMenu"), ':')[1],
                split(col("SessionMainMenu"), ':')[2],
                split(col("SessionMainMenu"), ':')[3]),
            concat_ws(':',
                split(col("SessionMainMenu"), ':')[4],
                split(col("SessionMainMenu"), ':')[5],
                split(col("SessionMainMenu"), ':')[6]))
    ).withColumn(
        "Millis",
        regexp_replace(trim(split(col("SessionMainMenu"), ':')[7]), ",", ".").cast("double")
    ).withColumn(
        "RealTimePlaying",
        regexp_replace(trim(col("RealTimePlaying")), ",", ".").cast("double")
    )

    df = df.withColumn(
        "SessionTimeStamp",
        try_to_timestamp(col("SessionTime"), lit('yyyy-MM-dd HH:mm:ss'))
    ).withColumn(
        "RealTimePlaying",
        when(isnan(col("RealTimePlaying")), 0.0)
        .otherwise(coalesce(col("RealTimePlaying"), lit(0.0)))
    )

    df = df.select(
        col("Mac").alias("userID"),
        col("AppName"),
        col("LogID"),
        col("Event"),
        col("ItemID"),
        col("RealTimePlaying"),
        col("SessionTimeStamp"),
        col("Millis")
    )

    return df
  
def read_log_files(folder_path):
  
  list_files = [ f for f in os.listdir(folder_path) if f.startswith("log")]
  
  print("danh sach files: ")
  print(list_files)
  
  df_all = None 
  
  for file_name in list_files:
    
    if df_all is None:
      df_all = convert_format(folder_path+'/'+file_name)
      
    else:
      
      df = convert_format(folder_path+'/'+file_name)
      
      df_all = df_all.union(df)
      
    print ("doc xong file: " + file_name)
    
  return df_all

df = read_log_files(folder_path=folder_path)

def read_user_info_file(file_path):
  
    raw = spark.read.text(file_path)
    
    fixed_raw = raw.withColumn("value", regexp_replace("value", r"\\t", "\t"))
    
    df = fixed_raw.withColumn("MAC", split(col("value"), '\t')[0]) \
                  .withColumn("days", split(col("value"), '\t')[1].cast("int"))

    df = df.withColumn("device_type", when(length(col("MAC")) == 16, substring(col("MAC"), 1, 4))) \
        .withColumn("userID", substring(col("MAC"), 5, 12)) 
                      
    return df.select(col("userID"), col("device_type"), col("days")).filter(df.device_type!="MAC")
  
df1 = read_user_info_file('./DataSampleTest/user_info.txt')

def write_parquet(df, out_path, name, mode = "overwrite"):
    full_path = f"{out_path.rstrip('/')}/{name}"

    try:
        df.write.mode(mode).parquet(full_path)
        print(f"DataFrame saved to Parquet at: {full_path}")
    except Exception as e:
        import traceback
        print("Error saving DataFrame to Parquet:")
        traceback.print_exc()
        print("="*50)
        print(str(e))
        
df2 = df1.join(df, on="userID", how="inner")

write_parquet(
    df=df,
    out_path=out_path+'/df',
    name="df",
    mode="overwrite"
)

write_parquet(df1, out_path+'/df1', "df1", "overwrite")
write_parquet(df2, out_path+'/df2', "df2", "overwrite")
