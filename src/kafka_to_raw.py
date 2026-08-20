import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_date,
    hour,
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

# 마지막으로 처리한 Kafka Offset 저장 위치
# 다음 Batch 실행 시 여기서 읽어서 이어서 처리
OFFSET_FILE = "/opt/spark/work-dir/output/state/kafka_offsets.json"


# --------------------------------------------------
# 1. Spark Session 생성
# --------------------------------------------------
#
# Structured Streaming이 아닌 일반 Spark Batch Job
# 실행 → Kafka 데이터 처리 → Bronze 저장 → 종료
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

    # Olist 원본 이벤트 발생 시간
    StructField("original_event_time", StringType(), True),

    # 현재 시간 기준으로 재구성한 이벤트 발생 시간
    StructField("event_time", StringType(), True),

    # Kafka Producer가 실제 전송한 시간
    StructField("ingestion_time", StringType(), True),

    StructField("order_status", StringType(), True)
])


# --------------------------------------------------
# 3. 이전 Batch의 Offset 확인
# --------------------------------------------------
#
# 첫 실행
# → earliest부터 시작
#
# 이후 실행
# → kafka_offsets.json에 저장된 Offset부터 시작
# --------------------------------------------------

starting_offsets = "earliest"

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
# 4. Kafka 데이터를 Batch 방식으로 읽기
# --------------------------------------------------
#
# readStream ❌
# read       ✅
#
# 실행 시점까지 Kafka에 들어온 데이터만 읽고 종료
# --------------------------------------------------

kafka_df = (
    spark.read
    .format("kafka")

    # Docker 내부에서 Kafka 접근
    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    )

    .option(
        "subscribe",
        TOPIC
    )

    # 이전 처리 지점부터 시작
    .option(
        "startingOffsets",
        starting_offsets
    )

    # Job 실행 시점의 최신 Offset까지만 처리
    .option(
        "endingOffsets",
        "latest"
    )

    .load()
)


# --------------------------------------------------
# 5. Kafka JSON 메시지 Parsing
# --------------------------------------------------

parsed_df = (
    kafka_df
    .select(

        # Kafka value(JSON String)를 컬럼 구조로 변환
        from_json(
            col("value").cast("string"),
            event_schema
        ).alias("event"),

        # 추적을 위해 Kafka Metadata도 Bronze에 보존
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
# 6. Bronze Metadata 생성
# --------------------------------------------------

raw_df = (
    parsed_df

    # Spark Batch가 실제 처리한 시간
    .withColumn(
        "processing_time",
        current_timestamp()
    )

    # Bronze 물리 Partition 기준
    .withColumn(
        "raw_date",
        to_date(col("processing_time"))
    )

    .withColumn(
        "raw_hour",
        hour(col("processing_time"))
    )

    # Kafka 유입 → Spark 처리까지 걸린 시간
    .withColumn(
    "lag_sec",
    unix_timestamp(col("processing_time"))
    - unix_timestamp(
        to_timestamp(
            col("ingestion_time"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
        )
    )
)


# --------------------------------------------------
# 7. 이번 Batch 데이터 고정
# --------------------------------------------------
#
# Spark는 Lazy Evaluation이므로
# 이후 여러 Action에서 같은 Batch 데이터를 사용하기 위해 Cache
# --------------------------------------------------

raw_df.cache()

batch_count = raw_df.count()


print("\n=== KAFKA BATCH COUNT ===")
print("rows:", batch_count)


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
#
# Bronze는 가공을 최소화하고 Append 방식으로 보존
#
# raw/
# └── raw_date=YYYY-MM-DD/
#     └── raw_hour=HH/
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
        RAW_PATH
    )
)


# --------------------------------------------------
# 10. 이번 Batch에서 처리한 마지막 Kafka Offset 확인
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
# 11. 다음 Batch의 시작 Offset 계산
# --------------------------------------------------
#
# 이번 Batch가 Offset 10까지 처리했다면
# 다음에는 11부터 읽어야 함
# --------------------------------------------------

next_offsets = {
    TOPIC: {}
}

for row in offset_rows:

    partition = str(row["partition"])
    next_offset = row["max_offset"] + 1

    next_offsets[TOPIC][partition] = next_offset


# --------------------------------------------------
# 12. Offset State 저장
# --------------------------------------------------
#
# 이 파일이 있어야 15분 뒤 다음 Job에서
# 이전 메시지를 다시 읽지 않고 이어서 처리 가능
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
# 13. 처리 결과 확인
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