from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.conf import SparkConf
import time

if __name__ == "__main__":
    spark = SparkSession\
    .builder.appName("WordCount").getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    #spark.sparkContext.setLogLevel("TRACE")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    schema = StructType([StructField("Time", LongType(), True),\
        StructField("Word", StringType(), True)])

    Df1 = spark\
        .readStream\
        .format("csv")\
        .option("header", True)\
        .option("path", "./data/spark_stream_dir")\
        .schema(schema)\
        .load()

    withTime = Df1.withColumn('Timestamp', from_unixtime('Time').cast('timestamp'))
    
    #wordCount = withTime.withWatermark('Timestamp', '1 seconds')\
    #    .groupBy(window(col('Timestamp'), "10 seconds", "1 second"), col('Word'))\
    #        .count()

       
    wordCount = withTime.withWatermark('Timestamp', '1 seconds')\
        .groupBy(col('Word'))\
            .count() 


    startTime = time.time()
    query = wordCount.writeStream.outputMode('complete').format("console").trigger(once=True).start().awaitTermination() 
    #query = wordCount.writeStream.format("console").start().awaitTermination() 
    endTime = time.time()
    print(endTime - startTime) 
 
    