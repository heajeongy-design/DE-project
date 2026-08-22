# E-commerce Order & Logistics Lakehouse

> Olist 주문 데이터를 이용한 Kafka → Spark Batch → Apache Iceberg 기반 데이터 파이프라인 프로젝트

## Documentation

- Daily Development Log (Notion)
  : https://app.notion.com/p/DE-Project-3c09cd4b60ef8086ad22d3ae0fe7d9a8?source=copy_link

---

# 1. Project Overview

이커머스 주문/배송 데이터를 이용하여 데이터가 수집되고,
Bronze → Silver → Gold로 가공되는 데이터 파이프라인을 구현하는 프로젝트이다.

Olist 데이터는 이미 수집이 끝난 과거 데이터이기 때문에
Python Producer를 이용해 주문 데이터를 이벤트 형태로 재구성하고 Kafka로 전달한다.

현재는 로컬 Docker 환경에서 다음 구간까지 구현하였다.

```text
Olist Dataset
      ↓
Python Event Producer
      ↓
Apache Kafka
      ↓
Spark Batch
      ↓
Bronze (Parquet)
      ↓
Spark Batch
      ↓
Silver (Apache Iceberg)
      ↓
Spark Batch
      ↓
Gold (Apache Iceberg)
```

이후 AWS S3 / Glue / Athena 연동과 Airflow 스케줄링,
Power BI 연결을 진행할 예정이다.

---

# 2. Domain

## E-commerce Order & Logistics

프로젝트 도메인은 이커머스 주문 및 물류 데이터로 선정하였다.

이커머스 주문은 생성 이후 여러 상태를 거친다.

```text
ORDER_CREATED
      ↓
PAYMENT_APPROVED
      ↓
SHIPPED
      ↓
DELIVERED
```

주문이 생성된 이후에도 결제, 배송, 취소 등의 이벤트가 발생하면서
동일한 주문의 상태가 계속 변경될 수 있다.

이러한 특성을 이용해 Kafka 이벤트 처리와
Iceberg의 MERGE 기능을 함께 실습할 수 있도록 구성하였다.

---

# 3. Dataset

## Olist Brazilian E-Commerce Dataset

Olist의 공개 이커머스 데이터셋을 사용하였다.

원본 데이터에는 다음과 같은 데이터가 포함되어 있다.

- Orders
- Customers
- Products
- Payments
- Sellers
- Order Items

Olist 데이터는 과거 주문 데이터이므로
Python을 이용해 주문 Lifecycle Event 형태로 변환하였다.

현재 생성하는 이벤트는 다음과 같다.

```text
ORDER_CREATED
PAYMENT_APPROVED
SHIPPED
DELIVERED
CANCELLED
```

원본 이벤트 발생 시각은 `original_event_time`으로 보존하고,
Producer가 이벤트를 재생하는 시점은 별도의 `event_time`으로 관리한다.

```text
Olist Historical Data
        ↓
Event 생성
        ↓
Python Producer
        ↓
Kafka order-events
```

---

# 4. Architecture

## Current

현재 구현된 범위는 다음과 같다.

```text
Olist Dataset
      ↓
Python Event Producer
      ↓
Apache Kafka
      ↓
Spark Batch
      ↓
Bronze Layer
Raw Parquet
      ↓
Spark Batch
      ↓
Silver Layer
Apache Iceberg V2
      ↓
Spark Batch
      ↓
Gold Layer
Apache Iceberg
```
초기 20건의 이벤트를 이용하여 kafka event flow를 검증하였고,
전체 Lifecycle Event 37,444건을 Kafka로 전송하여
해당 데이터 기준 파이프라인 처리를 검증하였다.

- 전체 Event : 37,444 건


## 진행 예정

```text
Gold Iceberg
      ↓
Amazon S3
      ↓
AWS Glue Data Catalog
      ↓
Amazon Athena
      ↓
Power BI
```

파이프라인 실행 자동화는 이후 Airflow를 추가하여 진행할 예정이다.

---

# 5. Processing Strategy

처음에는 Spark Structured Streaming 방식도 고려했지만,
현재 프로젝트에서는 초 단위 처리가 필요한 상황을 가정하지 않았다.

Kafka에 이벤트를 저장해두고 일정 주기로 Spark Batch를 실행하여
새로 들어온 데이터를 처리하는 방식으로 구성하였다.

현재 계획하고 있는 실행 주기는 다음과 같다.

| 구간 | 처리 방식 | 주기 |
|---|---|---|
| Event → Kafka | Python Producer | 이벤트 발생 시 |
| Kafka → Bronze | Spark Batch | 약 15분 |
| Bronze → Silver | Spark Batch | 약 15분 |
| Silver → Gold | Spark Batch | 추후 결정 |
| Iceberg Maintenance | Batch | 진행 예정 |

현재는 각 Job을 수동으로 실행하여 동작을 검증하고 있으며,
실제 주기 실행은 Airflow 단계에서 적용할 예정이다.

---

# 6. Technology Stack

| 구분 | Technology | 상태 |
|---|---|---|
| Data Source | Olist Dataset | 완료 |
| Event Simulation | Python | 완료 |
| Event Broker | Apache Kafka | 완료 |
| Processing | Apache Spark | 완료 |
| File Format | Apache Parquet | 완료 |
| Table Format | Apache Iceberg | 완료 |
| Storage | Amazon S3 | 진행 예정 |
| Catalog | AWS Glue Data Catalog | 진행 예정 |
| Query | Amazon Athena | 진행 예정 |
| BI | Power BI | 진행 예정 |
| Orchestration | Apache Airflow | 진행 예정 |

---

# 7. Medallion Architecture

데이터는 용도에 따라 Bronze → Silver → Gold로 구분하였다.

```text
Bronze
Raw Event 저장
      ↓
Silver
주문별 최신 상태 관리
      ↓
Gold
일별 주문 집계
```

---

## 7.1 Bronze - Raw Event Layer

Bronze에서는 Kafka에서 받은 이벤트를 Parquet 형태로 저장한다.

현재 구현된 주요 컬럼은 다음과 같다.

- event_id
- order_id
- customer_id
- event_type
- original_event_time
- event_time
- ingestion_time
- order_status
- topic
- partition
- offset
- kafka_timestamp
- processing_time
- lag_sec

Kafka의 `partition`, `offset`도 같이 저장한다.

### Partition

Bronze Parquet은 처리 날짜와 시간을 기준으로 저장한다.

```text
output/raw/
└── raw_date=YYYY-MM-DD/
    └── raw_hour=HH/
        └── *.parquet
```

### Offset 관리

Batch를 실행할 때마다 Kafka 데이터를 처음부터 읽지 않도록
Partition별 마지막 Offset을 저장한다.

```text
이전 Offset 확인
      ↓
새로운 Event만 조회
      ↓
Bronze 저장
      ↓
다음 Offset 저장
```

이전 실행에서 Offset 11까지 처리했다면
다음 실행에서는 Offset 12부터 읽는 방식이다.

현재 Kafka → Bronze 증분 처리까지 구현하였다.

---

## 7.2 Silver - Processed Order Layer

Silver에서는 Bronze의 이벤트를 주문 단위의 최신 상태로 정리한다.

현재 구현한 처리 과정은 다음과 같다.

```text
Bronze
      ↓
event_id 기준 중복 제거
      ↓
order_id별 최신 Event 선택
      ↓
Timestamp 변환
      ↓
current_status 생성
      ↓
Iceberg MERGE
      ↓
Silver
```

실제 적용한 내용:

- `event_id` 기준 중복 제거
- `order_id`별 최신 이벤트 1건 선택
  - `event_time DESC`
  - 동일한 경우 `offset DESC`
- `original_event_time` → TIMESTAMP
- `event_time` → TIMESTAMP
- `ingestion_time` → TIMESTAMP
- 최신 `event_type`을 `current_status`로 저장
- `event_time`에서 `event_date` 생성
- `order_id` 기준 MERGE

MERGE에서는 기존 주문보다 새로운 이벤트가 들어온 경우 UPDATE하고,
기존에 없는 주문이면 INSERT한다.

---

## 7.3 Silver Iceberg

Silver 테이블은 Apache Iceberg Format Version 2로 생성하였다.

```text
format-version = 2
```

주문 상태처럼 기존 Row의 변경이 필요한 데이터를 처리하기 위해
Silver 테이블에서 `MERGE INTO`를 사용한다.

현재 Silver에는 Merge-on-Read 설정을 적용하였다.

```text
write.update.mode = merge-on-read
write.merge.mode  = merge-on-read
write.delete.mode = merge-on-read
```

현재 로컬 Iceberg 테이블에서
Bronze → Silver MERGE 실행까지 확인하였다.

---

## 7.4 Gold - Business Summary Layer

Gold에서는 Silver 데이터를 일별로 집계한다.

현재 구현한 테이블은 다음과 같다.

```text
daily_order_summary
```

현재 계산하는 값:

- `total_orders`
- `created_orders`
- `approved_orders`
- `shipped_orders`
- `delivered_orders`
- `delivery_rate`

처리 과정은 다음과 같다.

```text
Silver Orders
      ↓
event_date 기준 GroupBy
      ↓
상태별 주문 수 집계
      ↓
delivery_rate 계산
      ↓
Gold MERGE
```

`delivery_rate`는 다음 기준으로 계산한다.

```text
delivered_orders / total_orders
```

Gold는 `order_date`를 기준으로 MERGE한다.

동일 날짜가 이미 존재하면 집계 값을 UPDATE하고,
없는 날짜라면 INSERT한다.

현재 `daily_order_summary` 테이블 생성과
Silver → Gold 집계 실행까지 확인하였다.

매출, 취소율, 평균 배송기간 등의 추가 KPI는
관련 데이터를 추가한 이후 확장할 예정이다.

---

# 8. Why Apache Iceberg?

일반적인 Parquet 파일만 사용해도 데이터를 저장할 수 있지만,
이번 프로젝트의 주문 데이터는 상태가 변경되는 특징이 있다.

예를 들어 하나의 주문이 다음과 같이 변경될 수 있다.

```text
order_1001
ORDER_CREATED
      ↓
PAYMENT_APPROVED
      ↓
SHIPPED
      ↓
DELIVERED
```

Bronze에서는 각각의 이벤트를 보존하지만
Silver에서는 주문별 현재 상태가 필요하다.

따라서 `order_id`를 기준으로 기존 데이터를 갱신할 수 있도록
Iceberg의 `MERGE INTO`를 사용하였다.

현재 프로젝트에서 Iceberg를 실제 적용한 부분은
Silver와 Gold 테이블이다.

---

# 9. Failure Recovery

현재 구현에서 재처리를 고려한 부분은 다음과 같다.

### Bronze Raw Event 보존

Kafka에서 받은 이벤트를 Bronze에 보존한다.

Silver 처리 로직을 수정하더라도
Bronze 데이터를 다시 읽어 Silver를 생성할 수 있다.

### Kafka Offset 관리

Kafka Partition별 마지막 처리 Offset을 저장하여
다음 Batch에서 신규 이벤트부터 처리한다.

### event_id 중복 제거

Silver 처리 시 `event_id`를 기준으로 중복 이벤트를 제거한다.

### Silver MERGE

`order_id` 기준으로 기존 주문과 신규 주문을 구분한다.

```text
기존 주문 + 최신 Event
→ UPDATE

신규 주문
→ INSERT
```

---

# 10. Iceberg Maintenance - 진행 예정

Silver에서 MERGE가 반복되면
파일과 Snapshot이 계속 증가할 수 있다.

따라서 이후 다음 작업을 추가할 예정이다.

- Compaction
- Snapshot Expiration
- 필요 시 Orphan File 정리

실제 데이터가 반복 적재되는 상황을 만든 후
파일 및 Snapshot 변화를 확인하면서 적용할 예정이다.

---

# 11. AWS 연동 - 진행 예정

현재 Bronze / Silver / Gold는 로컬 환경에서 구현하였다.

다음 단계에서는 AWS 환경으로 저장 계층을 확장할 예정이다.

진행 예정:

```text
Bronze / Silver / Gold
        ↓
Amazon S3
        ↓
AWS Glue Data Catalog
        ↓
Amazon Athena
```

Athena에서 Gold Iceberg 테이블을 조회할 수 있는 상태까지
구성하는 것이 다음 단계의 목표이다.

---

# 12. Airflow - 진행 예정

현재 Spark Job은 직접 실행하고 있다.

이후 Airflow를 이용해 다음 Job을 자동 실행하도록 구성할 예정이다.

```text
Kafka → Bronze
        ↓
Bronze → Silver
        ↓
Silver → Gold
```

추가로 Iceberg Maintenance Job도 별도 스케줄로 구성할 예정이다.

실패 알림 및 로그 확인 방식은
Airflow 구현 단계에서 함께 정리할 예정이다.

---

# 13. Power BI - 진행 예정

Gold 데이터를 Athena에서 조회할 수 있게 구성한 이후
Power BI 연결을 진행할 예정이다.

현재는 BI에서 사용할 데이터 구조를 만드는 단계까지 진행하였다.

Power BI에서는 우선 Gold의 일별 주문 집계를 이용하여
기본 주문 현황을 확인하는 대시보드를 구성할 예정이다.

추가 KPI는 실제 Gold 데이터가 확장되는 시점에 함께 추가한다.

---

# 14. Future Architecture

현재 목표는 우선 AWS 기반 파이프라인을 끝까지 구현하는 것이다.

```text
Kafka
  ↓
Spark Batch
  ↓
Bronze
  ↓
Silver Iceberg
  ↓
Gold Iceberg
  ↓
S3 / Glue
  ↓
Athena
  ↓
Power BI
```

Microsoft Fabric 연계는 현재 구현 범위에는 포함하지 않는다.

프로젝트 완료 이후 데이터 활용 범위를 확장할 필요가 있을 경우
별도의 확장 방향으로 검토할 예정이다.

---

# 15. Current Progress

## 완료

- [x] Olist 데이터 탐색
- [x] 주문 Lifecycle Event 생성
- [x] Docker 기반 Kafka 환경 구성
- [x] Kafka `order-events` Topic 구성
- [x] Python Kafka Producer 구현
- [x] Docker 기반 Spark 환경 구성
- [x] Kafka → Spark Batch 처리
- [x] Kafka Partition / Offset 기반 증분 처리
- [x] Bronze Raw Parquet 저장
- [x] `raw_date / raw_hour` Partition 구성
- [x] Silver `event_id` 중복 제거
- [x] Silver 주문별 최신 Event 선택
- [x] Silver Iceberg V2 테이블 생성
- [x] Silver Merge-on-Read 설정
- [x] Bronze → Silver MERGE 실행
- [x] Gold `daily_order_summary` 생성
- [x] Silver → Gold 일별 집계
- [x] Gold MERGE 실행 및 결과 확인

## 진행 예정

- [ ] SHIPPED / DELIVERED 이벤트 추가 검증
- [ ] Iceberg Compaction
- [ ] Snapshot Expiration
- [ ] Amazon S3 연동
- [ ] AWS Glue Data Catalog 연동
- [ ] Amazon Athena 조회
- [ ] Airflow Batch Scheduling
- [ ] Airflow 실패 처리 / 알림
- [ ] Power BI 연결 및 Dashboard 구성
