from pyspark.sql import SparkSession


# --------------------------------------------------
# 1. Spark + Iceberg Session 생성
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("CreateGoldIceberg")

    # Iceberg SQL 기능 사용
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
# 2. Gold Namespace 생성
# --------------------------------------------------

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS local.gold
""")


# --------------------------------------------------
# 3. Gold 일별 주문 집계 테이블 생성
# --------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS local.gold.daily_order_summary (

    -- 집계 기준 날짜
    order_date DATE,

    -- 전체 주문 수
    total_orders BIGINT,

    -- 상태별 주문 수
    created_orders BIGINT,
    approved_orders BIGINT,
    shipped_orders BIGINT,
    delivered_orders BIGINT,

    -- 배송 완료율
    delivery_rate DOUBLE,

    -- Gold 마지막 갱신 시간
    updated_at TIMESTAMP
)

USING iceberg

PARTITIONED BY (order_date)

TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
)
""")


# --------------------------------------------------
# 4. Gold 테이블 생성 결과 확인
# --------------------------------------------------

print("\n=== GOLD ICEBERG TABLE ===")

spark.sql("""
SHOW TABLES IN local.gold
""").show(truncate=False)


print("\n=== GOLD TABLE SCHEMA ===")

spark.table(
    "local.gold.daily_order_summary"
).printSchema()


spark.stop()