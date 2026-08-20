from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    unix_timestamp,
    current_timestamp,
    row_number
)
from pyspark.sql.window import Window


# --------------------------------------------------
# 1. Spark + Iceberg Session 생성
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("MergeSilverOrders")

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

    # 로컬 테스트 환경에서는 Hadoop Catalog 사용
    .config(
        "spark.sql.catalog.local.type",
        "hadoop"
    )

    # Iceberg Warehouse 위치
    .config(
        "spark.sql.catalog.local.warehouse",
        "/opt/spark/work-dir/warehouse"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Bronze Raw Parquet 읽기
# --------------------------------------------------

bronze_df = spark.read.parquet(
    "/opt/spark/work-dir/output/raw"
)


# --------------------------------------------------
# 3. Timestamp 타입 정리
# --------------------------------------------------

typed_df = (
    bronze_df

    # 실시간 시뮬레이션 이벤트 발생 시간
    .withColumn(
        "event_time",
        to_timestamp(col("event_time"))
    )

    # Kafka 유입 시간
    .withColumn(
        "ingestion_time",
        to_timestamp(col("ingestion_time"))
    )

    # Olist 원본 이벤트 시간
    .withColumn(
        "original_event_time",
        to_timestamp(col("original_event_time"))
    )
)


# --------------------------------------------------
# 4. event_id 기준 중복 제거
# --------------------------------------------------

# 같은 이벤트가 여러 번 들어온 경우
# 가장 최근 ingestion_time 1건만 유지
event_dedupe_window = (
    Window
    .partitionBy("event_id")
    .orderBy(col("ingestion_time").desc())
)

deduped_df = (
    typed_df
    .withColumn(
        "event_row_num",
        row_number().over(event_dedupe_window)
    )
    .filter(col("event_row_num") == 1)
    .drop("event_row_num")
)


# --------------------------------------------------
# 5. order_id별 최신 이벤트 선택
# --------------------------------------------------

# Silver는 주문 1건당 현재 상태 1행을 유지
# 따라서 같은 order_id 중 가장 최신 event_time을 선택
latest_order_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        col("event_time").desc(),
        col("ingestion_time").desc()
    )
)

latest_orders_df = (
    deduped_df
    .withColumn(
        "order_row_num",
        row_number().over(latest_order_window)
    )
    .filter(col("order_row_num") == 1)
    .drop("order_row_num")
)


# --------------------------------------------------
# 6. Silver MERGE용 컬럼 생성
# --------------------------------------------------

merge_source_df = (
    latest_orders_df

    # Silver 분석 기준 날짜
    .withColumn(
        "event_date",
        to_date(col("event_time"))
    )

    # 이벤트 발생 → Kafka 유입 지연시간
    .withColumn(
        "ingestion_lag_sec",
        unix_timestamp(col("ingestion_time"))
        - unix_timestamp(col("event_time"))
    )

    # Silver 마지막 갱신 시간
    .withColumn(
        "updated_at",
        current_timestamp()
    )

    # 기존 event의 order_status를 Silver의 current_status로 사용
    .withColumn(
        "current_status",
        col("order_status")
    )

    # Silver Table에 필요한 컬럼만 선택
    .select(
        "order_id",
        "customer_id",
        "event_id",
        "event_type",
        "original_event_time",
        "event_time",
        "ingestion_time",
        "current_status",
        "ingestion_lag_sec",
        "event_date",
        "updated_at"
    )
)


# --------------------------------------------------
# 7. MERGE Source를 Temporary View로 등록
# --------------------------------------------------

merge_source_df.createOrReplaceTempView(
    "silver_order_updates"
)


# --------------------------------------------------
# 8. Iceberg MERGE INTO 실행
# --------------------------------------------------

spark.sql("""
MERGE INTO local.silver.orders AS target

USING silver_order_updates AS source

ON target.order_id = source.order_id

-- 기존 주문이 있으면 최신 상태로 UPDATE
WHEN MATCHED THEN UPDATE SET
    target.customer_id = source.customer_id,
    target.event_id = source.event_id,
    target.event_type = source.event_type,
    target.original_event_time = source.original_event_time,
    target.event_time = source.event_time,
    target.ingestion_time = source.ingestion_time,
    target.current_status = source.current_status,
    target.ingestion_lag_sec = source.ingestion_lag_sec,
    target.event_date = source.event_date,
    target.updated_at = source.updated_at

-- 신규 주문이면 INSERT
WHEN NOT MATCHED THEN INSERT (
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
# 9. MERGE 결과 확인
# --------------------------------------------------

print("\n=== SILVER ROW COUNT ===")

spark.sql("""
SELECT COUNT(*) AS row_count
FROM local.silver.orders
""").show()


print("\n=== SILVER CURRENT STATUS ===")

spark.sql("""
SELECT
    current_status,
    COUNT(*) AS order_count
FROM local.silver.orders
GROUP BY current_status
ORDER BY order_count DESC
""").show(truncate=False)


print("\n=== SILVER SAMPLE ===")

spark.sql("""
SELECT
    order_id,
    event_type,
    current_status,
    event_time,
    ingestion_lag_sec,
    event_date,
    updated_at
FROM local.silver.orders
ORDER BY updated_at DESC
LIMIT 20
""").show(truncate=False)


spark.stop()