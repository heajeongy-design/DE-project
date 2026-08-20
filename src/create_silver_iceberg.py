from pyspark.sql import SparkSession


# --------------------------------------------------
# 1. Spark + Iceberg Session 생성
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("CreateSilverIceberg")

    # Iceberg의 MERGE / UPDATE / DELETE SQL 사용
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # local 이라는 이름의 Iceberg Catalog 생성
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # 로컬 테스트 환경에서는 Hadoop Catalog 사용
    # 운영 환경에서는 Glue Catalog + S3로 변경 예정
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
# 2. Silver Namespace 생성
# --------------------------------------------------

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS local.silver
""")


# --------------------------------------------------
# 3. Silver Iceberg Table 생성
# --------------------------------------------------
#
# 운영 전략
# - Bronze : Kafka 이벤트를 실시간 Append
# - Silver : 약 15분 단위 Micro-batch MERGE 가정
# - MOR    : 주문 상태 변경이 빈번하므로 쓰기 비용 절감 목적
# - Compaction : 일 1회 실행 예정
# - Snapshot Expiration : 일 1회 실행 예정
#
# Maintenance 작업은 별도 스크립트 / Airflow DAG로 관리한다.
# --------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS local.silver.orders (

    -- 주문 단위 MERGE Key
    order_id STRING,

    customer_id STRING,

    -- 이벤트 중복 제거용 Key
    event_id STRING,

    event_type STRING,

    -- Olist 원본 이벤트 발생 시간
    original_event_time TIMESTAMP,

    -- 실시간 시뮬레이션 이벤트 시간
    event_time TIMESTAMP,

    -- Kafka 유입 시간
    ingestion_time TIMESTAMP,

    -- Silver에서 관리할 주문 최신 상태
    current_status STRING,

    -- 이벤트 발생 → 수집 지연시간
    ingestion_lag_sec BIGINT,

    -- 분석 및 Partition 기준 날짜
    event_date DATE,

    -- Silver 레코드 마지막 갱신 시각
    updated_at TIMESTAMP
)

USING iceberg

-- 주문 이벤트 발생 날짜 기준 Partition
PARTITIONED BY (event_date)

TBLPROPERTIES (

    -- Row-level UPDATE / DELETE / MERGE 활용을 위해 Iceberg V2 사용
    'format-version' = '2',

    -- 주문 상태 변경이 자주 발생하는 Silver 특성상
    -- 변경사항을 즉시 Data File 전체 재작성하지 않는 MOR 선택
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',

    -- Small File이 지나치게 많이 생기지 않도록
    -- 목표 Data File 크기를 약 128MB로 설정
    'write.target-file-size-bytes' = '134217728'
)
""")


# --------------------------------------------------
# 4. 테이블 생성 결과 확인
# --------------------------------------------------

print("\n=== ICEBERG TABLE ===")

spark.sql("""
SHOW TABLES IN local.silver
""").show(truncate=False)


print("\n=== TABLE PROPERTIES ===")

spark.sql("""
SHOW TBLPROPERTIES local.silver.orders
""").show(100, truncate=False)


print("\n=== TABLE SCHEMA ===")

spark.table(
    "local.silver.orders"
).printSchema()


spark.stop()