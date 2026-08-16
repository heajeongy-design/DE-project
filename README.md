# E-commerce Order & Logistics Lakehouse

> Apache Iceberg 기반의 이커머스 주문·물류 이벤트 처리 파이프라인 구축 프로젝트

## 1. Project Overview

이 프로젝트는 이커머스의 주문 및 배송 이벤트를 수집하고,
대규모 데이터 환경에서도 안정적으로 변경 데이터를 처리할 수 있는
Lakehouse 아키텍처를 설계하고 구현하는 것을 목표로 한다.

단순히 데이터를 적재하고 시각화하는 것에 그치지 않고 다음과 같은 질문을 중심으로 설계하였다. (여전히 생각중)

- 주문/배송 데이터에서 Apache Iceberg가 필요한 이유는 무엇인가?
- 데이터가 지속적으로 증가할 경우 어떻게 확장할 것인가?
- 데이터 엔지니어뿐만 아니라 분석가와 현업 사용자도 쉽게 데이터를 활용하려면 어떻게 해야 하는가?
- 불필요한 데이터 복제와 연산을 줄이면서 분석 환경을 구성할 수 있는가?
- 장애 발생 시 데이터를 다시 처리할 수 있는 구조인가?


---

## 2. Domain Selection

### E-commerce Order & Logistics

프로젝트 도메인은 이커머스 주문 및 물류 데이터로 선정하였다.

이커머스 주문은 한 번 생성되고 끝나는 데이터가 아니다.

하나의 주문은 다음과 같이 여러 상태를 거칠 수 있다.

ORDER_CREATED
→ PAYMENT_APPROVED
→ SHIPPED
→ DELIVERED

또한 주문 취소, 배송 지연 등으로 인해 이미 저장된 주문 데이터의
상태가 이후 변경될 수 있다.

따라서 단순 Append 방식으로 데이터를 계속 추가하는 것보다
기존 데이터를 안정적으로 변경하고 이력을 관리할 수 있는
테이블 포맷이 필요하다고 판단하였다.


---

## 3. Dataset

### Olist Brazilian E-Commerce Dataset

실제 이커머스 환경과 유사한 데이터를 사용하기 위해
Olist의 공개 이커머스 데이터셋을 활용할 예정이다.

주요 데이터:

- Orders
- Customers
- Products
- Payments
- Sellers
- Order Items

Olist 데이터는 과거 데이터이기 때문에
Python Event Producer를 통해 주문 데이터를 이벤트 형태로 변환하여
실시간 이벤트가 발생하는 환경을 시뮬레이션한다.

예상 이벤트:

- ORDER_CREATED
- PAYMENT_APPROVED
- SHIPPED
- DELIVERED
- CANCELLED

※ 실제 이벤트 정의는 원본 데이터 분석 후 최종 확정한다.


---

# 4. Architecture

## Current Architecture

Olist Dataset
        ↓
Python Event Producer
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓ (Medallion Architecture)
Bronze Layer
        ↓
Silver Layer (Apache Iceberg)
        ↓
Gold Layer (Apache Iceberg)
        ↓
Amazon S3
        ↓
Amazon Athena
        ↓
Power BI


### Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Source | Olist Dataset | 실제 이커머스 데이터 |
| Event Simulation | Python | 과거 데이터를 이벤트 형태로 재생 |
| Event Streaming | Apache Kafka | 주문/배송 이벤트 스트리밍 |
| Processing | Apache Spark | 스트리밍 처리 및 데이터 변환 |
| Table Format | Apache Iceberg | Upsert, Snapshot, 테이블 관리 |
| Storage | Amazon S3 | 실제 데이터 파일 저장 |
| Catalog | AWS Glue Data Catalog | Iceberg 테이블 메타데이터 관리 |
| Query | Amazon Athena | Gold 데이터 SQL 조회 및 검증 |
| BI | Power BI | KPI 및 운영 대시보드 |


---

# 5. Medallion Architecture

데이터는 Bronze → Silver → Gold 3계층으로 분리

## Bronze - Raw Event Layer

Kafka에서 수집한 이벤트를 최대한 원본 형태로 저장

예상 데이터:

- event_id
- order_id
- event_type
- event_time
- raw_payload
- ingestion_time

Bronze 데이터를 보존하는 가장 중요한 이유는 재처리 가능성이다.

Silver 변환 로직에 오류가 발생하거나
새로운 비즈니스 로직이 추가되는 경우 원천 시스템에서 데이터를
다시 가져오는 대신 Bronze 데이터를 이용하여 재처리할 수 있도록 한다.


## Silver - Processed Order Layer

Bronze의 이벤트 데이터를 정제하여 분석 가능한 주문 단위 데이터로 만든다.

예상 테이블:

processed_orders

주요 컬럼:

- order_id
- customer_id
- order_status
- purchase_time
- approved_time
- shipped_time
- delivered_time
- amount
- updated_at

Silver Layer부터 Apache Iceberg를 적극적으로 활용한다.

동일한 order_id에 새로운 상태 이벤트가 도착하면
MERGE INTO를 이용하여 기존 주문 상태를 변경한다.


## Gold - Business Summary Layer

Silver의 상세 데이터를 BI에서 반복적으로 집계하지 않도록
분석에 필요한 KPI를 미리 계산한다.

예상 테이블:

daily_order_summary

주요 KPI:

- Total Orders
- Total Sales
- Cancellation Rate
- Average Delivery Time
- Delayed Delivery Rate

Power BI는 가능한 한 대규모 Silver 데이터를 직접 계산하지 않고
Gold Layer를 중심으로 조회하도록 설계한다.


---

# 6. Why Apache Iceberg? (★중요★)

## 문제

일반적인 S3 + Parquet 기반 Data Lake에서도 데이터를 저장하고
Athena를 통해 조회할 수 있다.

하지만 주문/배송 데이터는 상태가 계속 변경된다.

예:

order_1001 = ORDER_CREATED
        ↓
order_1001 = SHIPPED
        ↓
order_1001 = DELIVERED

따라서 단순 Append만으로 처리할 경우
동일 주문에 대한 여러 상태 레코드가 계속 쌓이게 되고,
현재 상태를 계산하기 위한 추가 로직이 필요하다.


## Iceberg를 선택한 이유

### 1. MERGE 기반 변경 데이터 처리

Iceberg 테이블에서는 새로운 주문은 INSERT하고,
기존 주문에 새로운 상태 이벤트가 발생하면 UPDATE하는
Upsert 패턴을 구현할 수 있다.

이를 통해 주문 상태가 지속적으로 변경되는 이커머스 도메인에
적합한 데이터 모델을 구성할 수 있다.


### 2. Snapshot 기반 데이터 관리

Iceberg는 테이블 변경 시 Snapshot을 관리한다.

이를 활용하여 데이터 변경 이력을 확인하고
운영 과정에서 특정 시점의 테이블 상태를 추적할 수 있는 구조를 만든다.


### 3. 데이터 증가에 대응하기 위한 테이블 관리

데이터가 지속적으로 들어오는 Streaming 환경에서는
작은 파일이 다수 생성되는 Small File 문제가 발생할 수 있다.

따라서 Iceberg의 Compaction과 Snapshot 관리 기능을 활용하여
장기적으로 데이터 파일과 메타데이터가 과도하게 증가하지 않도록
관리할 예정이다.


---

# 7. Cost & Performance Considerations

이 프로젝트에서는 단순히 많은 기술을 사용하는 것보다
불필요한 데이터 처리와 조회를 줄이는 것을 중요하게 고려하였다.

## 7.1 S3를 Storage Layer로 사용

대규모 Raw/Silver 데이터를 BI 도구 내부에 중복 저장하기보다
Amazon S3를 데이터의 주요 저장 계층으로 사용한다.

Iceberg는 별도의 데이터베이스 저장소를 대체하는 개념이 아니라,
S3에 저장된 Parquet Data File과 Metadata를 테이블 형태로 관리하는 역할을 담당한다.


## 7.2 Gold Layer를 통한 반복 연산 감소

Power BI에서 매번 대규모 Silver 데이터를 대상으로
주문 수, 매출, 배송 지연률 등을 계산하는 대신
Gold Layer에서 주요 KPI를 사전에 집계한다.

Silver
수억/수십억 rows
        ↓
Aggregation
        ↓
Gold
분석에 필요한 데이터
        ↓
Athena
        ↓
Power BI

이를 통해 BI 계층에서 처리해야 하는 데이터량과
반복적인 집계 연산을 줄이는 것을 목표로 한다.


## 7.3 Athena Query Cost 고려

Athena는 쿼리가 읽는 데이터량이 비용과 성능에 영향을 미치므로
불필요한 Scan을 줄이는 방향으로 데이터를 설계한다.

고려 사항:

- 적절한 Partition 전략
- Parquet Columnar Format
- Gold Layer 사전 집계
- 불필요한 SELECT * 지양
- Iceberg Metadata 활용

즉 단순히 "데이터를 저장하는 구조"가 아니라
실제 분석 시 읽어야 하는 데이터의 양까지 고려한다.


---

# 8. Why Power BI?

AWS 환경만 고려한다면 Amazon QuickSight도 자연스럽다고 판단하였다.

S3와 Athena를 중심으로 데이터가 구성되어 있기 때문에, 
AWS 환경 내부에서 분석까지 완료한다면 구조가 단순하며, 별도의 분석 플랫폼을 추가 하지 않아도 된다는 장점도 있음이 확실하다.


하지만 이 프로젝트를 설계하면서 데이터 파이프라인의 최종 사용자가 누구인가? 를 고민했다.

데이터 엔지니어의 역할은 데이터를 안정적으로 수집하고 처리하는 것에서 끝나지 않는다고 생각했다.
최종적으로 만들어진 데이터를 통해 협업이 더 빠르게 상황을 판단하고 문제를 해결할 수 있어야 
데이터 파이프ㅡ라인의 가치가 발생하는 것이 아닌가

또한 **'현업 사용자의 데이터 접근성'** 이 중요하다고 생각한다.
물류 현장에서 데이터를 사용하는 모든 사용자가 sql을 작성할 수 있는 것은 아니다. (경험상)
'특정 기간 배송 지연 증가 / 어떤 지역 배송 지연이 많이 발생하고 있는지' 현장에서 Ad-hoc 건으로 즉시 판단해야하는 이러한 상황에 
Athene에 데이터가 존재하면 빠르게 대답할 수 있을까?
(물론 자주 질의가 나오는 내용들은 KPI로 Dashboard 화 하면 되지만, 단발성에 대한 모든 경우의 수를 만들긴 현실적으로 불가)

질문을 확인하기 위해 매번 SQL 작성해야 한다면 비개발자에게는 데이터 접근 장벽이 높아질 수 있다.

*현업 질문 -> 데이터 담당자 요청 -> SQL 작성 -> 결과 추출 -> Excel 전달 -> 현업 확인

본 프로젝트에서는 단순 시각화를 넘어
향후 데이터 활용 범위를 데이터 엔지니어에서
현업 사용자가 이미 정제된 데이터를 스스로 탐색할 수 있는 환경을 고려하여 Power BI를 선택하였다.

현재 단계에서는 다음과 같은 KPI Dashboard를 구성한다.

- 주문 현황
- 매출
- 취소율
- 평균 배송기간
- 배송 지연률
- 데이터 적재 상태
- Iceberg 운영 상태

Power BI를 선택함으로써 향후 Microsoft Fabric의 Semantic Model과
연계하여 조직의 KPI 정의를 중앙화하는 구조로 확장할 수 있다.


---

# 9. Current vs Future Architecture

## Current

초기 프로젝트에서는 구현 복잡도와 비용을 최소화하기 위해
AWS 기반 데이터 파이프라인을 중심으로 구축한다.

AWS Gold Iceberg
        ↓
Amazon Athena
        ↓
Power BI


현재 규모에서는 별도의 분석 플랫폼을 추가하는 것보다
Athena를 통해 Gold 데이터를 직접 조회하는 것이
구조가 단순하고 관리해야 하는 구성 요소가 적다고 판단하였다.


## Future Architecture

향후 다음과 같은 상황을 가정한다.

- 데이터 규모 증가
- 데이터 갱신 빈도 증가
- Power BI 사용자 증가
- 데이터 분석가 증가
- Excel 기반 Ad-hoc 분석 요구 증가
- 여러 클라우드 및 업무 데이터와의 통합 분석 필요

이 경우 다음과 같은 분석 Serving Layer 확장을 고려한다.

AWS Gold / S3
        ↓
OneLake Shortcut
        ↓
Microsoft Fabric
        ↓
Direct Lake Semantic Model
        ↓
   ┌──────────────┐
   ↓              ↓
Power BI         Excel
Dashboard    Ad-hoc Analysis

