import findspark
import struct
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, from_json, window, expr
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, IntegerType, StructField


# I. IMPLEMENT A SPARL STREAMING APPLICATION THAT CONNECTS TO KAFKA CONSUMER.


findspark.init()

# Define the schema
schema = StructType([
    StructField('transaction_id', StringType()),
    StructField('timestamp', StringType()),
    StructField('customer_id', StringType()),
    StructField('merchant_id', StringType()),
    StructField('merchant_category', StringType()),
    StructField('payment_method', StringType()),
    StructField('amount', IntegerType()),
    StructField('location', StructType([
        StructField('lat', DoubleType()),
        StructField('lng', DoubleType())
    ])),
    StructField('device_info', StructType([
        StructField('os', StringType()),
        StructField('app_version', StringType()),
        StructField('device_model', StringType())
    ])),
    StructField('status', StringType()),
    StructField('commission_type', StringType()),
    StructField('commission_amount', IntegerType()),
    StructField('vat_amount', IntegerType()),
    StructField('total_amoun', IntegerType()),
    StructField('customer_type', StringType()),
    StructField('risk_level', IntegerType()),
    StructField('failure_reason', StringType())
])


# Build a new Spark session
spark_stream = SparkSession.builder \
    .appName('Fraud_Detection_System') \
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
df_json = df_spark_stream.selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), schema).alias('data')) \
    .select('data.*') \
    .withColumn('ingestion_time', col('timestamp').cast(TimestampType()))


# II. CALCULATING REAL_TIME METRICS FOR COMMISSIONS AND WRITE TO A KAFKA TOPIC. 


# Total commission by type per minute
df_total = df_json.withWatermark('ingestion_time', '1 minute') \
      .groupBy(window(col('ingestion_time'), '1 minute'), col('commission_type')) \
      .agg(sum('commission_amount').alias('total_commission'))

# Commission ratio by merchant category
df_commission = df_json.withWatermark('ingestion_time', '1 minute') \
      .groupBy(window(col('ingestion_time'), '1 minute'), col('merchant_category')) \
      .agg((sum('commission_amount') / sum('amount')).alias('commission_ratio'))

# Highest commission-generating merchants in 5-minute windows
df_highest = df_json.withWatermark('ingestion_time', '5 minutes') \
      .groupBy(window(col('ingestion_time'), '5 minutes'), col('merchant_id')) \
      .agg(sum('commission_amount').alias('commission_sum'))

# Show aggregated real-time data in console 
df_total.writeStream \
    .format('console') \
    .option('truncate', False) \
    .option('checkpointLocation', 'checkpoint/real/total/console') \
    .outputMode('append') \
    .start()
    
# Write first metric to a Kafka topic
df_total.select(to_json(struct('*')).alias('value')) \
    .writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('topic', 'darooghe.commission.total') \
    .option('checkpointLocation', 'checkpoint/real/total/kafka') \
    .start()

# Show aggregated real-time data in console 
df_commission.writeStream \
    .format('console') \
    .option('truncate', False) \
    .option('checkpointLocation', 'checkpoint/real/commission/console') \
    .outputMode('append') \
    .start()

# Write second metric to a Kafka topic
df_commission.select(to_json(struct('*')).alias('value')) \
    .writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('topic', 'darooghe.commission.ratio') \
    .option('checkpointLocation', 'checkpoint/real/commission/kafka') \
    .start()

# Show aggregated real-time data in console 
df_highest.writeStream \
    .format('console') \
    .option('truncate', False) \
    .option('checkpointLocation', 'checkpoint/real/highest/console') \
    .outputMode('append') \
    .start()

# Write third metric to a Kafka topic
df_highest.select(to_json(struct('*')).alias('value')) \
    .writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('topic', 'darooghe.commission.highest') \
    .option('checkpointLocation', 'checkpoint/real/highest/kafka') \
    .start()

# Run fo 60 seconds
spark_stream.streams.awaitAnyTermination(60)

print('Spark Streaming Application terminated successfully after 60 seconds.')

# Stop the Spark at then end
spark_stream.stop()