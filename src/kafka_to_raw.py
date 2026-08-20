import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_date,
    hour,
    to_timestamp,
    unix_timestamp,
    max as spark_max
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# --------------------------------------------------
# 0. 기본 설정
# --------------------------------------------------

TOPIC = "order-events"

# Bronze Raw Parquet 저장 위치
RAW_PATH = "/opt/spark/work-dir/output/raw"

# 다음 Batch에서 이어서 읽기 위한 Kafka Offset 저장 위치
OFFSET_FILE = "/opt/spark/work-dir/output/state/kafka_offsets.json"


# --------------------------------------------------
# 1. Spark Session 생성
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("KafkaToBronzeBatch")
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
# 3. 이전 Offset 불러오기
# --------------------------------------------------

starting_offsets = "earliest"
saved_offsets = None

if os.path.exists(OFFSET_FILE):

    with open(
        OFFSET_FILE,
        "r",
        encoding="utf-8"
    ) as f:
        saved_offsets = json.load(f)

    starting_offsets = json.dumps(saved_offsets)

    print("\n=== PREVIOUS OFFSETS ===")
    print(
        json.dumps(
            saved_offsets,
            indent=2
        )
    )

else:

    print("\n=== FIRST BATCH ===")
    print("저장된 Offset이 없어 earliest부터 시작합니다.")


# --------------------------------------------------
# 4. Kafka 데이터를 일반 Spark Batch로 읽기
# --------------------------------------------------

kafka_df = (
    spark.read
    .format("kafka")

    # Docker 내부 Kafka 주소
    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    )

    .option(
        "subscribe",
        TOPIC
    )

    # 이전 Batch에서 저장한 위치부터 시작
    .option(
        "startingOffsets",
        starting_offsets
    )

    # 현재 최신 Offset까지만 읽고 종료
    .option(
        "endingOffsets",
        "latest"
    )

    .load()
)


# --------------------------------------------------
# 5. Kafka JSON Parsing
# --------------------------------------------------

parsed_df = (
    kafka_df
    .select(

        # Kafka value(JSON)를 컬럼으로 변환
        from_json(
            col("value").cast("string"),
            event_schema
        ).alias("event"),

        # Kafka Metadata
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
# 6. Bronze용 컬럼 생성
# --------------------------------------------------

raw_df = (
    parsed_df

    # Spark Batch 실제 처리 시각
    .withColumn(
        "processing_time",
        current_timestamp()
    )

    # Bronze Partition 기준
    .withColumn(
        "raw_date",
        to_date(col("processing_time"))
    )

    .withColumn(
        "raw_hour",
        hour(col("processing_time"))
    )

    # Producer ingestion_time 문자열을 Timestamp로 변환
    .withColumn(
        "ingestion_timestamp",
        to_timestamp(
            col("ingestion_time"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
        )
    )

    # Kafka 유입 이후 Spark Batch 처리까지 걸린 시간
    .withColumn(
        "lag_sec",
        unix_timestamp(col("processing_time"))
        - unix_timestamp(col("ingestion_timestamp"))
    )
)


# --------------------------------------------------
# 7. 같은 DataFrame을 여러 번 사용하므로 Cache
# --------------------------------------------------

raw_df.cache()

batch_count = raw_df.count()


print("\n=== KAFKA BATCH COUNT ===")
print(f"rows: {batch_count}")


# --------------------------------------------------
# 8. 새로운 Kafka 데이터가 없으면 종료
# --------------------------------------------------

if batch_count == 0:

    print("\n새로운 Kafka 이벤트가 없습니다.")
    print("Bronze 적재 없이 Job을 종료합니다.")

    raw_df.unpersist()
    spark.stop()

    raise SystemExit(0)


# --------------------------------------------------
# 9. Bronze Raw Parquet 저장
# --------------------------------------------------

(
    raw_df.write

    # 기존 Bronze 데이터는 유지하고 신규 데이터만 추가
    .mode("append")

    .format("parquet")

    # 날짜 / 시간 기준 Partition
    .partitionBy(
        "raw_date",
        "raw_hour"
    )

    .save(
        RAW_PATH
    )
)


# --------------------------------------------------
# 10. 이번 Batch에서 처리한 Partition별 마지막 Offset 확인
# --------------------------------------------------

offset_rows = (
    raw_df
    .groupBy(
        "topic",
        "partition"
    )

    .agg(
        spark_max("offset").alias("max_offset")
    )

    .collect()
)


# --------------------------------------------------
# 11. 다음 Batch 시작 Offset 계산
# --------------------------------------------------

next_offsets = {
    TOPIC: {}
}

for row in offset_rows:

    partition = str(row["partition"])

    # 이번에 offset 10까지 읽었다면
    # 다음 실행은 11부터 시작
    next_offsets[TOPIC][partition] = (
        row["max_offset"] + 1
    )


# --------------------------------------------------
# 12. 다음 실행을 위해 Offset 저장
# --------------------------------------------------

os.makedirs(
    os.path.dirname(OFFSET_FILE),
    exist_ok=True
)

with open(
    OFFSET_FILE,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        next_offsets,
        f,
        indent=2
    )


print("\n=== NEXT STARTING OFFSETS ===")
print(
    json.dumps(
        next_offsets,
        indent=2
    )
)


# --------------------------------------------------
# 13. Bronze 적재 결과 확인
# --------------------------------------------------

print("\n=== BRONZE BATCH SAMPLE ===")

raw_df.select(
    "event_id",
    "order_id",
    "event_type",
    "partition",
    "offset",
    "event_time",
    "ingestion_time",
    "processing_time",
    "lag_sec"
).show(
    20,
    truncate=False
)


# --------------------------------------------------
# 14. Batch Job 종료
# --------------------------------------------------

raw_df.unpersist()

spark.stop()

print("\nKafka → Bronze Batch Job 완료")