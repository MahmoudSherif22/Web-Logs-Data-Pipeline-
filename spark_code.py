from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, to_timestamp, window, col
# Start Spark Session
def startSparkSession():
    return (SparkSession.builder
            .appName("WebLogs")
            .master("local[*]")
            .getOrCreate())
# Read stream data from kafka 
def ReadKafkaTopic(spark):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "web2")
            .option("startingOffsets", "latest")
            .load())
#Parse data from binary to string 
def parse_data(df):
    return df.selectExpr("CAST(value AS STRING) AS json_str")
#Convert data from string to table 
def convertDataToTableFormat(df):
    parsed = df.select(
        regexp_extract(col("json_str"), r'^(\S+)', 1).alias("ip"),
        regexp_extract(col("json_str"), r'\[(.*?)\]', 1).alias("timestamp"),
        regexp_extract(col("json_str"), r'\"(\S+)\s(\S+)\s(\S+)\"', 1).alias("method"),
        regexp_extract(col("json_str"), r'\"(\S+)\s(\S+)\s(\S+)\"', 2).alias("url"),
        regexp_extract(col("json_str"), r'\"(\S+)\s(\S+)\s(\S+)\"', 3).alias("protocol"),
        regexp_extract(col("json_str"), r'\s(\d{3})\s', 1).alias("status")
    )
    # create event-time column once
    return parsed.withColumn("ts", to_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss Z"))
# apply aggregation on 5 minutes micro batch 
def AnalyzeData(df):
    df_wm = df.withWatermark("ts", "5 minutes")
    statusCount = df_wm.groupBy("status").count()  # Count result for each status
    traffic = df_wm.groupBy(window("ts", "1 minute")).count() # Count visitors for last 5 minutes
    topUrls = df_wm.groupBy(window("ts", "1 minute"), "url").count() # know the most visisted url
    return statusCount, topUrls, traffic

def showDataInConsole(df):
    return (df.writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .start())

if __name__ == "__main__":
    spark = startSparkSession()

    df_kafka = ReadKafkaTopic(spark)
    df_string = parse_data(df_kafka)
    df_logs = convertDataToTableFormat(df_string)
    df_statusCount, df_topUrl, df_traffic = AnalyzeData(df_logs)

    q1 = showDataInConsole(df_statusCount)
    q2 = showDataInConsole(df_topUrl)
    q3 = showDataInConsole(df_traffic)

    # Wait AFTER all streams are started
    spark.streams.awaitAnyTermination()
