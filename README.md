# E-commerce Order & Logistics Lakehouse

> Apache Iceberg 기반의 이커머스 주문·물류 이벤트 처리 Lakehouse 프로젝트

## Documentation

- Daily Development Log (Notion)

---

# 1. Project Overview

이 프로젝트는 이커머스의 주문 및 배송 이벤트를 수집하고,
데이터 변경과 지속적인 데이터 증가를 안정적으로 처리할 수 있는
Lakehouse 아키텍처를 설계하고 구현하는 것을 목표로 한다.

단순히 데이터를 적재하고 시각화하는 것에 그치지 않고
다음과 같은 문제를 중심으로 설계하였다.

- 주문/배송 데이터에서 Apache Iceberg가 필요한 이유는 무엇인가?
- 데이터가 지속적으로 증가할 경우 어떻게 확장할 것인가?
- 주문 상태가 변경될 때 기존 데이터를 어떻게 효율적으로 갱신할 것인가?
- Batch Job이 반복 실행될 때 이미 처리한 데이터를 어떻게 다시 읽지 않을 것인가?
- 장애 발생 시 데이터를 다시 처리할 수 있는 구조를 어떻게 만들 것인가?
- 반복적인 데이터 처리와 조회 비용을 어떻게 줄일 것인가?
- 데이터 엔지니어뿐만 아니라 분석가와 현업 사용자도 쉽게 데이터를 활용할 수 있는가?
- 장기 운영 시 Small File과 Snapshot을 어떻게 관리할 것인가?
- 파이프라인 실패를 어떻게 감지하고 운영자가 확인할 수 있도록 할 것인가?

---

# 2. Domain Selection

## E-commerce Order & Logistics

프로젝트 도메인은 **이커머스 주문 및 물류 데이터**로 선정하였다.

이커머스 주문은 한 번 생성되고 끝나는 데이터가 아니다.

하나의 주문은 다음과 같이 여러 상태를 거칠 수 있다.

```text
ORDER_CREATED
      ↓
PAYMENT_APPROVED
      ↓
SHIPPED
      ↓
DELIVERED
```

또한 주문 취소, 배송 지연 등의 상황으로 인해
이미 저장된 주문의 상태가 이후 변경될 수 있다.

따라서 단순 Append 방식으로 데이터를 계속 추가하는 것보다
기존 주문 상태를 안정적으로 갱신하고 변경 데이터를 관리할 수 있는
테이블 구조가 필요하다고 판단하였다.

이러한 데이터 특성이 Apache Iceberg의 `MERGE`, Snapshot,
Partition 관리 기능을 실습하기에 적합하다고 판단하여
프로젝트 도메인으로 선정하였다.

---

# 3. Dataset

## Olist Brazilian E-Commerce Dataset

실제 이커머스 환경과 유사한 데이터를 사용하기 위해
Olist의 공개 이커머스 데이터셋을 활용한다.

주요 데이터는 다음과 같다.

- Orders
- Customers
- Products
- Payments
- Sellers
- Order Items

Olist는 과거 주문 데이터이므로 실제 운영 환경처럼 이벤트가
지속적으로 발생하지 않는다.

따라서 Python Event Producer를 통해 과거 주문 데이터를
이벤트 형태로 변환하여 주문 Lifecycle을 재현한다.

현재 사용하는 주요 이벤트는 다음과 같다.

```text
ORDER_CREATED
PAYMENT_APPROVED
SHIPPED
DELIVERED
CANCELLED
```

이를 통해 다음과 같은 흐름을 시뮬레이션한다.

```text
Olist Historical Data
        ↓
Python Event Producer
        ↓
Order Lifecycle Events
        ↓
Kafka
```

---

# 4. Architecture

## Current Architecture

현재 프로젝트의 전체 목표 아키텍처는 다음과 같다.

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
Gold Layer
Apache Iceberg
      ↓
Amazon S3
      ↓
Amazon Athena
      ↓
Power BI
```

현재 구현 단계에서는 로컬 Docker 환경에서
Kafka → Spark → Bronze 파이프라인을 구성하고,
Silver Iceberg 테이블 구조와 운영 전략을 설계하였다.

AWS S3, Glue Data Catalog, Athena, Gold Layer 및 BI 연결은
후속 단계에서 구현한다.

---

## Processing Strategy

초기 설계에서는 Spark Structured Streaming을 고려하였다.

그러나 현재 프로젝트에서는 초 단위 실시간 처리가 필요한 요구사항보다
약 15분 단위의 데이터 갱신을 가정하고 있다.

이 경우 Streaming Job을 계속 실행하는 것보다
주기적으로 Spark Batch Job을 실행하는 구조가
운영 복잡도와 리소스 관리 측면에서 적합하다고 판단하였다.

현재 처리 전략은 다음과 같다.

| 구간 | 처리 방식 | 계획 주기 |
|---|---|---|
| Event 발생 | Python → Kafka | 지속적 |
| Kafka → Bronze | Spark Batch + offset 증분 처리 | 약 15분 |
| Bronze → Silver | Spark Batch + MERGE | 약 15분 |
| Silver → Gold | Spark Batch | 1시간 또는 일 단위 |
| Iceberg Maintenance | Compaction / Snapshot Expiration | 일 단위 |

Kafka는 이벤트를 수집하고 보관하는 역할을 담당하고,
Spark Batch Job은 실행 시점까지 쌓인 신규 이벤트를 처리한다.

---

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Source | Olist Dataset | 이커머스 주문/물류 원본 데이터 |
| Event Simulation | Python | 과거 데이터를 Lifecycle Event로 재구성 |
| Event Broker | Apache Kafka | 주문 이벤트 수집 및 버퍼링 |
| Processing | Apache Spark | Batch 기반 증분 처리 및 데이터 변환 |
| Table Format | Apache Iceberg | MERGE, Snapshot, 테이블 관리 |
| File Format | Apache Parquet | Columnar 데이터 저장 |
| Storage | Amazon S3 | 데이터 파일 저장 |
| Catalog | AWS Glue Data Catalog | Iceberg 테이블 메타데이터 관리 |
| Query | Amazon Athena | Gold 데이터 SQL 조회 및 검증 |
| BI | Power BI | KPI 및 운영 대시보드 |
| Orchestration | Apache Airflow | Batch 및 Maintenance 스케줄링 예정 |

---

# 5. Medallion Architecture

데이터는 목적에 따라 **Bronze → Silver → Gold** 3계층으로 분리한다.

```text
Bronze
Raw Event 보존
      ↓
Silver
정제 + 주문 상태 관리
      ↓
Gold
BI용 KPI 집계
```

---

## Bronze - Raw Event Layer

Bronze는 Kafka에서 수집한 주문 이벤트를
최대한 원본에 가까운 형태로 보존하는 계층이다.

현재 Kafka 데이터를 Spark Batch로 읽어
Parquet 형식으로 저장하도록 구현하였다.

주요 컬럼은 다음과 같다.

- event_id
- order_id
- customer_id
- event_type
- event_time
- ingestion_time
- topic
- partition
- offset
- kafka_timestamp
- processing_time
- lag_sec

Kafka Metadata인 `partition`과 `offset`도 함께 저장하여
어떤 Kafka 데이터가 처리되었는지 추적할 수 있도록 한다.

### Partition Strategy

Bronze 데이터는 처리 날짜와 시간을 기준으로 Partition한다.

```text
output/raw/
└── raw_date=YYYY-MM-DD/
    └── raw_hour=HH/
        └── *.parquet
```

### Offset Management

Spark Batch가 실행될 때 Kafka 전체 데이터를 반복해서 읽지 않도록
Partition별 마지막 처리 Offset을 관리한다.

```text
Previous Offset 확인
        ↓
신규 Kafka Event 조회
        ↓
Spark Batch 처리
        ↓
Bronze Parquet 저장
        ↓
Next Offset 저장
```

예를 들어 이전 실행에서 Partition 0의 Offset 11까지 처리했다면
다음 실행에서는 Offset 12부터 처리한다.

이를 통해 Batch Job을 반복 실행하면서도
신규 이벤트만 증분 처리할 수 있도록 구성하였다.

### Why Keep Bronze?

Bronze 데이터를 보존하는 가장 중요한 이유는 **재처리 가능성**이다.

Silver 변환 로직에 오류가 발생하거나
새로운 비즈니스 로직이 추가되는 경우,
원천 데이터를 다시 수집하지 않고 Bronze 데이터를 이용하여
Silver Layer를 다시 생성할 수 있도록 한다.

---

## Silver - Processed Order Layer

Silver에서는 Bronze 이벤트를 정제하여
분석 및 주문 상태 관리에 사용할 수 있는 데이터로 변환한다.

주요 처리 대상은 다음과 같다.

- `event_id` 기반 중복 제거
- Timestamp 정규화
- 이벤트 데이터 정제
- `order_id` 기준 주문 최신 상태 관리
- `MERGE INTO` 기반 Upsert

Bronze에는 전체 Lifecycle Event를 보존하고,
Silver에서는 분석에 사용할 주문의 정제된 상태를 관리하는 방향으로 설계하였다.

```text
Bronze Events

ORDER_CREATED
PAYMENT_APPROVED
SHIPPED
DELIVERED

        ↓

Spark Batch

        ↓

Silver Orders

order_id → current_status
```

---

## Silver Iceberg Strategy

Silver Layer부터 Apache Iceberg를 적용한다.

현재 Silver 테이블은 **Iceberg Format Version 2**를 기준으로 설계하였다.

```text
format-version = 2
```

Format Version 2는 Row-level 변경을 지원하므로
주문 상태처럼 기존 데이터에 대한 `UPDATE`, `DELETE`, `MERGE`가
필요한 데이터 처리에 활용할 수 있다.

### Merge-on-Read

Silver는 주문 상태 변경이 반복적으로 발생하는 데이터 특성을 고려하여
Merge-on-Read(MOR) 전략을 적용하도록 설계하였다.

```text
write.update.mode = merge-on-read
write.merge.mode  = merge-on-read
write.delete.mode = merge-on-read
```

Merge-on-Read는 변경이 발생할 때마다 기존 Data File 전체를
즉시 다시 작성하는 대신 변경 정보를 별도로 관리하고,
조회 시 이를 병합하는 방식이다.

따라서 변경이 잦은 Silver Layer에서
쓰기 비용을 줄이는 방향으로 활용할 수 있다.

다만 MOR은 Delete File 및 Small File 증가 가능성이 있으므로
Maintenance 전략과 함께 운영해야 한다.

---

## Gold - Business Summary Layer

Gold는 Power BI 등 분석 도구에서 반복적으로 계산해야 하는
비즈니스 KPI를 미리 집계하는 계층이다.

예상 테이블:

```text
daily_order_summary
```

주요 KPI:

- Total Orders
- Total Sales
- Cancellation Rate
- Average Delivery Time
- Delayed Delivery Rate

Power BI에서 매번 대규모 Silver 데이터를 직접 집계하지 않고
Gold Layer에서 계산된 데이터를 중심으로 조회하도록 설계한다.

Gold Layer는 후속 단계에서 구현할 예정이다.

---

# 6. Why Apache Iceberg?

## Problem

일반적인 S3 + Parquet 기반 Data Lake에서도
데이터를 저장하고 Athena를 통해 조회할 수 있다.

하지만 주문/배송 데이터는 상태가 계속 변경된다.

```text
order_1001 = ORDER_CREATED
        ↓
order_1001 = PAYMENT_APPROVED
        ↓
order_1001 = SHIPPED
        ↓
order_1001 = DELIVERED
```

단순 Append 방식만 사용할 경우 동일한 주문에 대한
여러 상태 레코드가 계속 누적된다.

따라서 현재 주문 상태를 조회할 때마다
가장 최신 레코드를 찾는 추가 로직이 필요하다.

---

## 1. MERGE 기반 변경 데이터 처리

Iceberg를 사용하면 새로운 주문은 INSERT하고,
기존 주문에 새로운 상태 이벤트가 발생하면 UPDATE하는
Upsert 패턴을 구현할 수 있다.

```text
New Order
    → INSERT

Existing Order + New Status
    → UPDATE
```

이를 `MERGE INTO`를 통해 처리하여
상태 변경이 반복되는 주문 데이터를 관리한다.

---

## 2. Snapshot 기반 데이터 관리

Iceberg는 테이블 변경 시 Snapshot을 관리한다.

이를 통해 테이블이 어떻게 변경되었는지 추적하고
특정 시점의 데이터 상태를 확인할 수 있는 기반을 제공한다.

또한 잘못된 데이터 처리나 로직 변경이 발생했을 때
데이터 변경 이력을 추적하는 데 활용할 수 있다.

---

## 3. Partition Evolution

데이터 규모와 조회 패턴이 변경될 경우
Partition 전략 역시 변경될 가능성이 있다.

Iceberg는 Partition Evolution을 지원하므로
기존 데이터를 전체 재작성하지 않고
향후 Partition 전략을 변경할 수 있는 구조를 제공한다.

---

## 4. Schema Evolution

운영 과정에서 새로운 컬럼이 추가되거나
데이터 구조가 변경될 가능성이 있다.

Iceberg의 Schema Evolution 기능을 활용하면
테이블 구조 변화에 대응하기 용이하다.

---

# 7. Iceberg Maintenance Strategy

Iceberg를 사용하는 것만으로
테이블 운영 문제가 자동으로 해결되는 것은 아니다.

주기적인 Batch 적재와 MERGE가 반복되면
Small File, Delete File, Snapshot 및 Metadata가 증가할 수 있다.

따라서 데이터 처리 Job과 별도로
Maintenance Job을 운영하는 구조를 고려한다.

```text
15분
Kafka → Bronze

15분
Bronze → Silver MERGE

1시간 / 일 단위
Silver → Gold

일 단위
Iceberg Maintenance
```

주요 Maintenance 작업은 다음과 같다.

### Compaction

작은 Data File을 더 큰 파일로 병합하여
조회 시 읽어야 하는 파일 수를 줄인다.

### Snapshot Expiration

오래된 Snapshot을 정리하여
불필요한 Metadata가 계속 증가하는 것을 방지한다.

### Orphan File Cleanup

더 이상 현재 테이블 Metadata에서 참조하지 않는
불필요한 파일을 정리한다.

Maintenance는 데이터 처리 로직과 분리하여
향후 Airflow DAG를 통해 주기적으로 실행하는 방향으로 설계한다.

---

# 8. Failure Recovery & Idempotency

Batch 파이프라인에서는 동일한 데이터가 다시 처리되거나
중간 단계에서 Job이 실패할 가능성을 고려해야 한다.

현재 구조에서는 다음 요소를 통해 재처리 가능성을 확보한다.

### Bronze Raw Data 보존

원본 이벤트를 Bronze에 유지하여
Silver 로직에 문제가 발생하더라도 다시 처리할 수 있도록 한다.

### Kafka Offset 관리

Partition별 마지막 처리 Offset을 관리하여
다음 Batch에서 처리해야 할 위치를 추적한다.

### event_id 기반 중복 제거

동일 이벤트가 재처리되더라도
Silver에서 `event_id`를 기준으로 중복 데이터를 제거할 수 있도록 설계한다.

### MERGE 기반 주문 상태 관리

Silver에서는 `order_id`를 기준으로 기존 주문과 신규 이벤트를 비교하여
INSERT 또는 UPDATE하는 구조를 사용한다.

---

# 9. Cost & Performance Considerations

이 프로젝트에서는 단순히 많은 기술을 사용하는 것보다
불필요한 데이터 처리와 조회를 줄이는 것을 중요하게 고려하였다.

---

## 9.1 S3 as Storage Layer

대규모 Raw/Silver 데이터를 BI 도구 내부에 중복 저장하기보다
Amazon S3를 주요 Storage Layer로 사용한다.

Iceberg는 별도의 데이터베이스 저장소 자체를 의미하는 것이 아니라,
S3에 저장된 Parquet Data File과 Metadata를
테이블 형태로 관리하는 Table Format이다.

---

## 9.2 Batch Processing

현재 요구사항에서는 초 단위 데이터 처리가 필요하지 않으므로
장시간 실행되는 Streaming Job 대신
주기적인 Spark Batch Job을 사용하는 방향을 선택하였다.

이를 통해 현재 프로젝트 규모에서
불필요한 상시 연산 리소스와 운영 복잡도를 줄이는 것을 목표로 한다.

---

## 9.3 Gold Layer를 통한 반복 연산 감소

Power BI에서 매번 대규모 Silver 데이터를 대상으로
주문 수, 매출, 배송 지연률 등을 계산하는 대신
Gold Layer에서 주요 KPI를 사전에 집계한다.

```text
Silver
대규모 상세 데이터
        ↓
Aggregation
        ↓
Gold
BI용 집계 데이터
        ↓
Athena
        ↓
Power BI
```

이를 통해 BI 계층에서 처리해야 하는 데이터량과
반복적인 집계 연산을 줄이는 것을 목표로 한다.

---

## 9.4 Athena Query Cost

Athena는 쿼리가 읽는 데이터량이
비용과 성능에 직접적인 영향을 미친다.

따라서 불필요한 Scan을 줄이는 방향으로 데이터를 설계한다.

주요 고려 사항:

- 적절한 Partition 전략
- Parquet Columnar Format
- Gold Layer 사전 집계
- 불필요한 `SELECT *` 지양
- Iceberg Metadata 활용

즉 단순히 데이터를 저장하는 구조가 아니라
**실제 분석 시 얼마나 많은 데이터를 읽어야 하는가**까지 고려한다.

---

# 10. Why Power BI?

AWS 환경만 고려한다면 Amazon QuickSight를 사용하는 방법도 있다.

S3와 Athena를 중심으로 데이터가 구성되어 있기 때문에
AWS 내부에서 분석까지 완료한다면
아키텍처를 단순하게 유지할 수 있다는 장점이 있다.

그러나 이 프로젝트에서는 데이터 파이프라인의
최종 사용자를 데이터 엔지니어로 한정하지 않았다.

물류 및 이커머스 운영 환경에서는 다음과 같은
Ad-hoc 분석 요구가 발생할 수 있다.

```text
특정 기간 배송 지연이 증가했는가?

어떤 지역에서 배송 지연이 많이 발생하는가?

최근 취소율이 증가한 상품 또는 지역은 어디인가?
```

모든 질문을 사전에 Dashboard KPI로 구성하는 것은 현실적으로 어렵다.

반대로 질문이 발생할 때마다 다음 과정을 반복한다면
비개발자의 데이터 접근성이 낮아진다.

```text
현업 질문
   ↓
데이터 담당자 요청
   ↓
SQL 작성
   ↓
결과 추출
   ↓
Excel 전달
   ↓
현업 확인
```

따라서 본 프로젝트에서는 정제된 데이터를
Dashboard뿐만 아니라 향후 현업 사용자가 직접 탐색할 수 있는
분석 환경까지 확장하는 것을 고려하여 Power BI를 선택하였다.

예상 KPI Dashboard는 다음과 같다.

- 주문 현황
- 매출
- 취소율
- 평균 배송기간
- 배송 지연률
- 데이터 적재 상태
- Iceberg 운영 상태

---

# 11. Current vs Future Architecture

## Current

초기 프로젝트에서는 구현 복잡도와 비용을 최소화하기 위해
AWS 기반 데이터 파이프라인을 중심으로 구축한다.

```text
Gold Iceberg / S3
        ↓
Amazon Athena
        ↓
Power BI
```

현재 규모에서는 별도의 분석 플랫폼을 추가하는 것보다
Athena를 통해 Gold 데이터를 조회하는 것이
구조가 단순하고 관리해야 하는 구성 요소가 적다고 판단하였다.

---

## Future Architecture

향후 다음과 같은 상황을 가정한다.

- 데이터 규모 증가
- 데이터 갱신 빈도 증가
- Power BI 사용자 증가
- 데이터 분석가 증가
- Excel 기반 Ad-hoc 분석 요구 증가
- 여러 클라우드 및 업무 데이터와의 통합 분석 필요

이 경우 분석 Serving Layer 확장을 고려한다.

```text
AWS Gold / S3
        ↓
OneLake Shortcut
        ↓
Microsoft Fabric
        ↓
Semantic Model
        ↓
   ┌──────────────┐
   ↓              ↓
Power BI         Excel
Dashboard    Ad-hoc Analysis
```

현재 단계에서는 이러한 구조를 바로 구축하지 않고,
데이터 규모와 사용자 요구가 증가할 경우 적용할 수 있는
확장 방향으로 고려한다.

---

# 12. Pipeline Operation Plan

최종적으로 다음과 같은 운영 구조를 목표로 한다.

```text
                  ┌────────────────────┐
                  │ Python Producer    │
                  └─────────┬──────────┘
                            ↓
                        Kafka
                            ↓
                  ┌────────────────────┐
                  │ Spark Batch        │
                  │ Kafka → Bronze     │
                  └─────────┬──────────┘
                            ↓
                         Bronze
                            ↓
                  ┌────────────────────┐
                  │ Spark Batch        │
                  │ Bronze → Silver    │
                  │ Dedup + MERGE      │
                  └─────────┬──────────┘
                            ↓
                    Silver Iceberg
                            ↓
                  ┌────────────────────┐
                  │ Gold Aggregation   │
                  └─────────┬──────────┘
                            ↓
                     Gold Iceberg
                            ↓
                         Athena
                            ↓
                       Power BI


별도 운영 Job

Airflow
   ├── Batch Scheduling
   ├── Compaction
   ├── Snapshot Expiration
   └── Failure Alert
```

파이프라인 실패 알림과 에러 로그 분석 자동화는
후속 단계에서 Airflow를 기반으로 구현할 예정이다.

---

# 13. Current Progress

현재까지 구현 및 설계한 범위는 다음과 같다.

- [x] Olist 데이터 탐색
- [x] 주문 Lifecycle Event 생성
- [x] Docker 기반 Kafka 구성
- [x] Kafka `order-events` Topic 구성
- [x] Python Kafka Producer 구현
- [x] Docker 기반 Spark 환경 구성
- [x] Kafka → Spark Batch 처리
- [x] Kafka Partition / Offset 기반 증분 처리
- [x] Bronze Raw Parquet 저장
- [x] `raw_date / raw_hour` Partition 구성
- [x] Silver 데이터 정제 로직 작성
- [x] Iceberg Format Version 2 전략 설계
- [x] Silver Merge-on-Read 전략 설계
- [ ] Silver Iceberg MERGE 실행 및 검증
- [ ] Iceberg Compaction 구현
- [ ] Snapshot Expiration 구현
- [ ] Gold Layer 구현
- [ ] AWS S3 / Glue Catalog 연동
- [ ] Athena 조회
- [ ] Airflow Scheduling 및 실패 알림
- [ ] Power BI Dashboard
