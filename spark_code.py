from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract,to_timestamp, window,col
import datetime
from pyspark.sql.types import DateType


# start spark session 
def startSparkSession():
    spark = SparkSession\
    .builder.appName("WebLogs")\
    .master("local[*]")\
    .getOrCreate()
    return spark 

#read data from kafka topic 
def ReadKafkaTopic(spark):
    df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web2") \
    .load()
    return df 
    
#parse data from binary to json format 

def parse_data(df):
    df_str = df.selectExpr("CAST(value AS STRING) as json_str")
    return df_str

#convert data from string to table format 

def convertDataToTableFormat(df):
    parsed_df = df.select(
        regexp_extract(col("json_str"),r'^(\S+)',1).alias("ip"),
        regexp_extract(col("json_str"),r'\[(.*?)\]',1).alias("timestamp"),
        regexp_extract(col("json_str"),r'\"(\S+)\s(\S+)\s(\S+)\"',1).alias("method"),
        regexp_extract(col("json_str"),r'\"(\S+)\s(\S+)\s(\S+)\"',2).alias("url"),
        regexp_extract(col("json_str"),r'\"(\S+)\s(\S+)\s(\S+)\"',3).alias("protocol"),
        regexp_extract(col("json_str"), r'\s(\d{3})\s', 1).alias("status")
    )
    return parsed_df
    
def showDataInConsole(df):
    query = df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
    query.awaitTermination()

    
#Analyze the data 

def AnalyzeData(df):
    statusCount = df.groupby('status').count() #count requests per status code
    topUrls = df.groupby('url').count().orderBy('count' ,ascending = False)
    df = df.withColumn("ts", to_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss Z"))
    traffic = df.groupBy(window("ts", "1 minute")).count()
    return statusCount,topUrls,traffic

if __name__ == "__main__":

    #start spark session
    spark = startSparkSession()
    #Read Kafka Topic
    df_kafka = ReadKafkaTopic(spark)
    #parse data 
    df_string = parse_data(df_kafka)
    #convert to table format 
    df_logs = convertDataToTableFormat(df_string)
    #get the analyzed data 
    df_statusCount,df_topUrl,df_traffic = AnalyzeData(df_logs)
    #show data in console 
    showDataInConsole(df_statusCount)
    showDataInConsole(df_topUrl)
    showDataInConsole(df_traffic)
