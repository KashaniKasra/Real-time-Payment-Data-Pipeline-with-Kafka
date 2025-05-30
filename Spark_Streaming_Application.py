import findspark
import struct
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, col, to_json, to_timestamp, from_json, window
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, IntegerType, StructField


# I. IMPLEMENT A SPARL STREAMING APPLICATION THAT CONNECTS TO KAFKA CONSUMER.


findspark.init()

# Define the schema
schema = StructType([
    StructField('transaction_id', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('merchant_category', StringType(), True),
    StructField('total_amount', DoubleType(), True),
])

# Build a new Spark session
spark_stream = SparkSession.builder \
    .appName('Spark_Streaming_Application') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .config('spark.hadoop.io.native.lib.available', 'false') \
    .config('spark.hadoop.io.nativeio.enabled', 'false') \
    .getOrCreate()

# Set logs label to WARN
spark_stream.sparkContext.setLogLevel('WARN')

# Read stream from Kafka
df_spark_stream = spark_stream.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'darooghe.transactions') \
    .option('startingOffsets', 'latest') \
    .option('failOnDataLoss', 'false') \
    .load()

# Parse Kafka JSON with schema
df_json = df_spark_stream.selectExpr('CAST(value AS STRING) as json_str') \
    .select(from_json(col('json_str'), schema).alias('data')) \
    .select('data.*') \
    .withColumn('ingestion_time', to_timestamp('timestamp'))


# II. GET INSIGHTS FROM REAL-TIME MICRO-BATCH, 1 MINUTE WINDOWS, WITH 20 SECONDS SLIDING INTERVALS. 
# III. IMPLEMENT A CHECKPOINT MECHANISM FOR FAULT TOLERANCE.


# Specify the windows and sliding times
window_time = '1 minute'
sliding_time = '20 seconds'

# Create a minute windowed aggregation and delete old transactions
minute_windowed = df_json.withWatermark('ingestion_time', '2 minutes') \
    .groupBy(
        window(col('ingestion_time'), window_time, sliding_time),
        col('merchant_category')
    ).agg(count('*').alias('transactions_count'), sum('total_amount').alias("transaction_total_amount"))

# Show aggregated real-time data in console 
minute_windowed.writeStream \
    .format('console') \
    .option('truncate', False) \
    .option('checkpointLocation', 'checkpoint/spark/console') \
    .outputMode('append') \
    .start()

# Write the results in a Kafka topic
minute_windowed.writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('topic', 'darooghe.spark_streaming') \
    .option('checkpointLocation', 'checkpoint/spark/kafka') \
    .outputMode('append') \
    .start()

# Run fo 60 seconds
spark_stream.streams.awaitAnyTermination(60)

print('Spark Streaming Application terminated successfully after 60 seconds.')

# Stop the Spark at then end
spark_stream.stop()