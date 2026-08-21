# Week 4 - Silver & Gold Layer DDL

- **Silver Layer**: 주문별 최신 상태를 관리하는 정제 테이블
- **Gold Layer**: Silver 데이터를 기반으로 일별 KPI를 제공하는 집계 테이블

---

## 1. Silver Layer DDL

Silver Layer는 Bronze의 이벤트 데이터를 정제하여  
`order_id`별 최신 주문 상태를 관리한다.

Iceberg V2와 Merge-on-Read 방식을 적용하여  
이후 주문 상태 변경 시 `MERGE INTO`를 통해 Update/Insert할 수 있도록 설계하였다.

<그외 bronze_to_silver.py 기준 실제 처리>
: event_id 기준 중복 제거 설정
: order_id 별 event_time , offset 기준 최신 이벤트 1건 선택
: original_event_time, event_time, ingestion_time을 TIMESTAMP로 변환

```sql
CREATE TABLE IF NOT EXISTS local.silver.orders (

    order_id STRING,
    customer_id STRING,
    event_id STRING,
    event_type STRING,

    original_event_time TIMESTAMP,
    event_time TIMESTAMP,
    ingestion_time TIMESTAMP,

    current_status STRING,
    ingestion_lag_sec BIGINT,

    event_date DATE,
    updated_at TIMESTAMP
)

USING iceberg

PARTITIONED BY (event_date)

TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'write.target-file-size-bytes' = '134217728'
);
```

---

## 2. Gold Layer DDL

Gold Layer는 Silver의 주문 데이터를 일 단위로 집계하여  
BI 및 분석에서 사용할 KPI를 저장한다.

<현재 구현한 KPI>

- total_orders     : 전체 주문 수
- created_orders   : 주문 생성 상태 건수
- approved_orders  : 결제 승인 상태 건수
- shipped_orders   : 배송 시작 상태 건수
- delivered_orders : 배송 완료 상태 건수
- delivery_rate    : 전체 주문 대비 배송 완료 비율

```sql
CREATE TABLE IF NOT EXISTS local.gold.daily_order_summary (

    order_date DATE,

    total_orders BIGINT,

    created_orders BIGINT,
    approved_orders BIGINT,
    shipped_orders BIGINT,
    delivered_orders BIGINT,

    delivery_rate DOUBLE,

    updated_at TIMESTAMP
)

USING iceberg

PARTITIONED BY (order_date)

TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);
```

---

## Table Structure

```text
Bronze Raw Events
        ↓
Silver
local.silver.orders
- 주문별 최신 상태
- Iceberg V2
- Merge-on-Read
        ↓
Gold
local.gold.daily_order_summary
- 일별 주문 KPI
```
