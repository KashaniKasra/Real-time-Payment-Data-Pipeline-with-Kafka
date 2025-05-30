import findspark
import struct
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, from_json, window, expr
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, IntegerType, StructField


# I. IMPLEMENT A SPARL STREAMING APPLICATION THAT CONNECTS TO KAFKA CONSUMER.


findspark.init()

# Define the schema
schema = StructType([
    StructField('transaction_id', StringType(), True),
    StructField('timestamp', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('amount', IntegerType(), True),
    StructField('location', StructType([
        StructField('lat', DoubleType(), True),
        StructField('lng', DoubleType(), True)
    ]), True),
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


# II. DETECTING FRAUD TRANSACTIONS USING THAT 3 RULES.


# Velocity check
df_velocity = df_json.withWatermark('event_time', '2 minutes') \
    .groupBy(window(col('ingestion_time'), '2 minutes'), col('customer_id')) \
    .count() \
    .filter(col('count') > 5)

# Geographical impossibility
df_geograpgical = df_json.alias('a').join(df_json.alias('b'), expr("""
        a.customer_id = b.customer_id
        AND a.ingestion_time BETWEEN b.ingestion_time AND b.ingestion_time + INTERVAL 5 minutes
        AND (6371*2*ASIN(
          SQRT(
            POWER(SIN(RADIANS(a.location.lat-b.location.lat)/2),2)
            + COS(RADIANS(b.location.lat))*COS(RADIANS(a.location.lat))*
              POWER(SIN(RADIANS(a.location.lng-b.location.lng)/2),2)
           )
        )) > 50
      """)).select(
         col('a.transaction_id').alias('transaction1'),
         col('b.transaction_id').alias("transaction2"),
         col('a.customer_id'),
         col('a.ingestion_time').alias('time1'),
         col('b.ingestion_time').alias('time2')
      )

# Amount anomaly
df_amount = df_json.join(spark_stream.createDataFrame([('customer1', 1000.0)], ['customer_id', 'avg_amount']), 'customer_id') \
    .filter(col('amount') > col('avg_amount')*10) \
    .select(
      col('transaction_id').alias('transaction1'),
      col('customer_id'),
      col('amount').alias('amount1'),
      col('avg_amount').alias('customer_avg')
    )

# Unuin and merge all these 3 rules and dataframes
df_fraud = df_velocity.unionByName(df_geograpgical, allowMissingColumns=True) \
    .unionByName(df_amount, allowMissingColumns=True)

# Show aggregated real-time data in console 
df_fraud.writeStream \
    .format('console') \
    .option('truncate', False) \
    .option('checkpointLocation', 'checkpoint/fraud/kafka') \
    .outputMode('append') \
    .start()

# Write detected fraud events to a Kafka topic
df_fraud.select(to_json(struct('*')).alias('value')) \
    .writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('topic', 'darooghe.fraud_alerts') \
    .option('checkpointLocation', 'checkpoint/fraud/kafka') \
    .start()

# Run fo 60 seconds
spark_stream.streams.awaitAnyTermination(60)

print('Spark Streaming Application terminated successfully after 60 seconds.')

# Stop the Spark at then end
spark_stream.stop()