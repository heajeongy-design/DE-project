from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
    current_timestamp
)


# --------------------------------------------------
# 1. Spark + Iceberg Session 생성
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SilverToGold")

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

    # Iceberg Warehouse 위치
    .config(
        "spark.sql.catalog.local.warehouse",
        "/opt/spark/work-dir/warehouse"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Silver 주문 데이터 읽기
# --------------------------------------------------

silver_df = spark.table(
    "local.silver.orders"
)


# --------------------------------------------------
# 3. 일별 Gold KPI 집계
# KPI
# - total_orders     : 전체 주문 수
# - created_orders   : 주문 생성 상태 건수
# - approved_orders  : 결제 승인 상태 건수
# - shipped_orders   : 배송 시작 상태 건수
# - delivered_orders : 배송 완료 상태 건수
# - delivery_rate    : 전체 주문 대비 배송 완료 비율
# --------------------------------------------------

gold_df = (
    silver_df

    .groupBy(
        col("event_date").alias("order_date")
    )

    .agg(
        # 전체 주문 수
        count("*").alias("total_orders"),

        # 상태별 주문 수
        spark_sum(
            when(
                col("current_status") == "ORDER_CREATED",
                1
            ).otherwise(0)
        ).alias("created_orders"),

        spark_sum(
            when(
                col("current_status") == "PAYMENT_APPROVED",
                1
            ).otherwise(0)
        ).alias("approved_orders"),

        spark_sum(
            when(
                col("current_status") == "SHIPPED",
                1
            ).otherwise(0)
        ).alias("shipped_orders"),

        spark_sum(
            when(
                col("current_status") == "DELIVERED",
                1
            ).otherwise(0)
        ).alias("delivered_orders")
    )

    # 배송 완료율
    .withColumn(
        "delivery_rate",
        when(
            col("total_orders") > 0,
            col("delivered_orders") / col("total_orders")
        ).otherwise(0.0)
    )

    # Gold 갱신 시간
    .withColumn(
        "updated_at",
        current_timestamp()
    )
)


# --------------------------------------------------
# 4. Gold 집계 결과 확인
# --------------------------------------------------

print("\n=== GOLD SOURCE ===")

gold_df.show(
    50,
    truncate=False
)


# --------------------------------------------------
# 5. MERGE용 Temporary View 생성
# --------------------------------------------------

gold_df.createOrReplaceTempView(
    "gold_updates"
)


# --------------------------------------------------
# 6. Silver → Gold MERGE
# --------------------------------------------------

spark.sql("""
MERGE INTO local.gold.daily_order_summary AS target

USING gold_updates AS source

ON target.order_date = source.order_date

WHEN MATCHED THEN UPDATE SET
    target.total_orders     = source.total_orders,
    target.created_orders   = source.created_orders,
    target.approved_orders  = source.approved_orders,
    target.shipped_orders   = source.shipped_orders,
    target.delivered_orders = source.delivered_orders,
    target.delivery_rate    = source.delivery_rate,
    target.updated_at       = source.updated_at

WHEN NOT MATCHED THEN INSERT (
    order_date,
    total_orders,
    created_orders,
    approved_orders,
    shipped_orders,
    delivered_orders,
    delivery_rate,
    updated_at
)

VALUES (
    source.order_date,
    source.total_orders,
    source.created_orders,
    source.approved_orders,
    source.shipped_orders,
    source.delivered_orders,
    source.delivery_rate,
    source.updated_at
)
""")


# --------------------------------------------------
# 7. Gold 결과 확인
# --------------------------------------------------

print("\n=== GOLD RESULT ===")

spark.sql("""
SELECT *
FROM local.gold.daily_order_summary
ORDER BY order_date
""").show(
    100,
    truncate=False
)


spark.stop()