from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    unix_timestamp,
    row_number
)
from pyspark.sql.window import Window


# Spark 실행 세션 생성
spark = (
    SparkSession.builder
    .appName("PrepareSilverOrders")   # Spark Job 이름
    .getOrCreate()
)

# 불필요한 INFO 로그를 줄이고 WARN 이상만 출력
spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 1. Bronze Raw Parquet 읽기
# --------------------------------------------------

bronze_df = spark.read.parquet(
    "/opt/spark/work-dir/output/raw"   # Kafka → Spark Streaming으로 저장한 Bronze 경로
)


# --------------------------------------------------
# 2. Timestamp 컬럼 타입 변환
# --------------------------------------------------

typed_df = (
    bronze_df

    # 문자열로 들어온 event_time을 Spark Timestamp 타입으로 변환
    .withColumn(
        "event_time",
        to_timestamp(col("event_time"))
    )

    # 문자열로 들어온 ingestion_time도 Timestamp 타입으로 변환
    .withColumn(
        "ingestion_time",
        to_timestamp(col("ingestion_time"))
    )
)


# --------------------------------------------------
# 3. event_id 기준 중복 제거
# --------------------------------------------------

# 같은 event_id가 여러 번 들어왔을 경우
# ingestion_time이 가장 최근인 데이터 1건만 남기기 위한 Window
dedupe_window = (
    Window
    .partitionBy("event_id")                       # event_id별로 그룹화
    .orderBy(col("ingestion_time").desc())         # 최신 ingestion_time 우선
)

deduped_df = (
    typed_df

    # 같은 event_id 안에서 최신순으로 1, 2, 3... 번호 부여
    .withColumn(
        "row_num",
        row_number().over(dedupe_window)
    )

    # 최신 데이터 1건만 유지
    .filter(col("row_num") == 1)

    # 중복 제거용 임시 컬럼 삭제
    .drop("row_num")
)


# --------------------------------------------------
# 4. Silver에서 사용할 파생 컬럼 생성
# --------------------------------------------------

silver_input_df = (
    deduped_df

    # event_time에서 날짜만 추출
    # 이후 Iceberg Partition 기준으로 활용 예정
    .withColumn(
        "event_date",
        to_date(col("event_time"))
    )

    # 이벤트 발생 시점과 파이프라인 유입 시점 간 차이 계산
    # Late Event / Streaming 지연 확인용
    .withColumn(
        "ingestion_lag_sec",
        unix_timestamp(col("ingestion_time"))
        - unix_timestamp(col("event_time"))
    )
)


# --------------------------------------------------
# 5. 처리 결과 검증
# --------------------------------------------------

print("\n=== BRONZE COUNT ===")
print(
    bronze_df.count()   # 중복 제거 전 Bronze 전체 건수
)

print("\n=== AFTER DEDUPE ===")
print(
    silver_input_df.count()   # event_id 중복 제거 후 건수
)

print("\n=== SILVER INPUT SCHEMA ===")
silver_input_df.printSchema()   # Silver에 들어갈 데이터 타입 확인


print("\n=== SILVER INPUT SAMPLE ===")

silver_input_df.select(
    "event_id",
    "order_id",
    "event_type",
    "event_time",
    "ingestion_time",
    "ingestion_lag_sec",
    "event_date"
).show(
    20,
    truncate=False
)


# Spark 세션 종료
spark.stop()