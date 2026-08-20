from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_date,
    hour,
    unix_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# --------------------------------------------------
# 1. Spark Session 생성
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("KafkaToRawBatch")   # Streaming이 아니라 Batch Job
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Kafka Event Schema 정의
# --------------------------------------------------

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("original_event_time", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("order_status", StringType(), True)
])


# --------------------------------------------------
# 3. Kafka 데이터를 Batch 방식으로 읽기
# --------------------------------------------------#
# Job 실행 시점까지 Kafka에 쌓인 데이터를 읽고 처리한 뒤 종료한다.
# --------------------------------------------------

kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "order-events")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)


# --------------------------------------------------
# 4. Kafka JSON 메시지 Parsing
# --------------------------------------------------

parsed_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            event_schema
        ).alias("event"),

        # Kafka Metadata도 같이 보존
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp")
    )
    .select(
        "event.*",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp"
    )
)


# --------------------------------------------------
# 5. Bronze용 Metadata 생성
# --------------------------------------------------

raw_df = (
    parsed_df

    # 현재 Spark Batch Job 처리 시간
    .withColumn(
        "processing_time",
        current_timestamp()
    )

    # Bronze Partition용 날짜
    .withColumn(
        "raw_date",
        to_date(col("processing_time"))
    )

    # Bronze Partition용 시간
    .withColumn(
        "raw_hour",
        hour(col("processing_time"))
    )

    # Kafka 유입 이후 Spark 처리까지 걸린 시간
    .withColumn(
        "lag_sec",
        unix_timestamp(col("processing_time"))
        - unix_timestamp(col("ingestion_time"))
    )
)


# --------------------------------------------------
# 6. Raw Parquet 저장
# --------------------------------------------------

(
    raw_df.write
    .mode("append")
    .format("parquet")
    .partitionBy(
        "raw_date",
        "raw_hour"
    )
    .save(
        "/opt/spark/work-dir/output/raw"
    )
)


# --------------------------------------------------
# 7. 처리 결과 확인
# --------------------------------------------------

print("\n=== KAFKA BATCH COUNT ===")
print("rows:", raw_df.count())

print("\n=== BATCH SAMPLE ===")

raw_df.select(
    "event_type",
    "order_id",
    "partition",
    "offset",
    "event_time",
    "ingestion_time",
    "lag_sec"
).show(20, truncate=False)


# Batch Job이므로 처리 후 종료
spark.stop()