from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    row_number,
    current_timestamp,
    to_date
)
from pyspark.sql.window import Window


# --------------------------------------------------
# 1. Spark + Iceberg Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("BronzeToSilver")

    # Iceberg MERGE INTO 사용
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # local Iceberg Catalog
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # 로컬 테스트용 Hadoop Catalog
    .config(
        "spark.sql.catalog.local.type",
        "hadoop"
    )

    # Iceberg Data / Metadata 저장 위치
    .config(
        "spark.sql.catalog.local.warehouse",
        "/opt/spark/work-dir/warehouse"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Bronze 데이터 읽기
# --------------------------------------------------

bronze_path = "/opt/spark/work-dir/output/raw"

bronze_df = (
    spark.read
    .option("basePath", bronze_path)
    .parquet(bronze_path)
)

print("\n=== BRONZE COUNT ===")
print("rows:", bronze_df.count())


# --------------------------------------------------
# 3. event_id 기준 중복 제거
# --------------------------------------------------
#
# 동일 이벤트가 재수집되더라도 Silver에서는 1건만 사용
# --------------------------------------------------

dedup_df = bronze_df.dropDuplicates(["event_id"])

print("\n=== AFTER EVENT_ID DEDUP ===")
print("rows:", dedup_df.count())


# --------------------------------------------------
# 4. order_id별 가장 최신 Event 선택
# --------------------------------------------------
#
# 하나의 주문에
#
# ORDER_CREATED
# → PAYMENT_APPROVED
# → SHIPPED
# → DELIVERED
#
# 이벤트가 여러 건 존재할 수 있으므로
# event_time이 가장 최신인 Event 1건을 선택
# --------------------------------------------------

window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(
        col("event_time").desc(),
        col("offset").desc()
    )
)

latest_df = (
    dedup_df

    .withColumn(
        "row_num",
        row_number().over(window_spec)
    )

    .filter(
        col("row_num") == 1
    )

    .drop("row_num")
)


# --------------------------------------------------
# 5. Silver Schema 생성
# --------------------------------------------------

silver_source_df = (
    latest_df
    .select(
        col("order_id"),
        col("customer_id"),
        col("event_id"),
        col("event_type"),

        # Bronze STRING → Silver TIMESTAMP
        col("original_event_time")
            .cast("timestamp")
            .alias("original_event_time"),

        col("event_time")
            .cast("timestamp")
            .alias("event_time"),

        col("ingestion_time")
            .cast("timestamp")
            .alias("ingestion_time"),

        # 최신 이벤트를 현재 주문 상태로 사용
        col("event_type").alias("current_status"),

        col("lag_sec")
            .cast("long")
            .alias("ingestion_lag_sec"),

        # Partition 기준 날짜
        to_date(
            col("event_time").cast("timestamp")
        ).alias("event_date"),

        current_timestamp().alias("updated_at")
    )
)


print("\n=== SILVER SOURCE SAMPLE ===")

silver_source_df.show(
    20,
    truncate=False
)


# --------------------------------------------------
# 6. MERGE용 Temporary View 생성
# --------------------------------------------------

silver_source_df.createOrReplaceTempView(
    "silver_updates"
)


# --------------------------------------------------
# 7. Bronze → Silver MERGE
# --------------------------------------------------
#
# order_id가 이미 존재하면 UPDATE
# 새로운 order_id이면 INSERT
#
# 단, 기존 Silver보다 새로운 Event인 경우에만 UPDATE
# --------------------------------------------------

spark.sql("""
MERGE INTO local.silver.orders AS target

USING silver_updates AS source

ON target.order_id = source.order_id

WHEN MATCHED
AND source.event_time >= target.event_time

THEN UPDATE SET

    target.customer_id       = source.customer_id,
    target.event_id          = source.event_id,
    target.event_type        = source.event_type,
    target.original_event_time = source.original_event_time,
    target.event_time        = source.event_time,
    target.ingestion_time    = source.ingestion_time,
    target.current_status    = source.current_status,
    target.ingestion_lag_sec = source.ingestion_lag_sec,
    target.event_date        = source.event_date,
    target.updated_at        = source.updated_at

WHEN NOT MATCHED

THEN INSERT (
    order_id,
    customer_id,
    event_id,
    event_type,
    original_event_time,
    event_time,
    ingestion_time,
    current_status,
    ingestion_lag_sec,
    event_date,
    updated_at
)

VALUES (
    source.order_id,
    source.customer_id,
    source.event_id,
    source.event_type,
    source.original_event_time,
    source.event_time,
    source.ingestion_time,
    source.current_status,
    source.ingestion_lag_sec,
    source.event_date,
    source.updated_at
)
""")


# --------------------------------------------------
# 8. Silver 결과 확인
# --------------------------------------------------

print("\n=== SILVER RESULT ===")

spark.sql("""
SELECT
    order_id,
    event_id,
    current_status,
    event_time,
    ingestion_lag_sec,
    event_date,
    updated_at
FROM local.silver.orders
ORDER BY event_time DESC
""").show(
    50,
    truncate=False
)


print("\n=== SILVER COUNT ===")

spark.sql("""
SELECT COUNT(*) AS silver_rows
FROM local.silver.orders
""").show()


spark.stop()