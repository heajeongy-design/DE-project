# Week 3 - Kafka → Spark Batch → Bronze Pipeline

## 목표

Olist 주문 데이터를 실제 주문 이벤트처럼 재구성하여 Kafka로 전송하고,
Spark Batch Job을 통해 신규 이벤트를 증분 수집하여 Bronze Layer에
Parquet 형식으로 적재하는 파이프라인을 구현하였다.

---

## Architecture

```text
Olist Sample Data
        ↓
Python Event Producer
        ↓
Kafka (order-events)
        ↓
Spark Batch
        ↓
Bronze Raw Parquet
```
---

[Kafka / Spark 실행 환경]

<img width="1091" height="219" alt="image" src="https://github.com/user-attachments/assets/647c6693-bd7b-436e-8725-8f624d1f14fe" />

[Kafka Event Producer]

<img width="847" height="882" alt="image" src="https://github.com/user-attachments/assets/510514e3-aca0-4e5c-87bd-af5f9aabd19e" />

[Kafka Consumer 확인]

<img width="1907" height="256" alt="image" src="https://github.com/user-attachments/assets/bad601ba-85f0-49fe-b505-7242fb9fd211" />

[Spark Batch → Bronze 적재]

<img width="1874" height="924" alt="image" src="https://github.com/user-attachments/assets/c8546289-7fcc-403f-a9b3-b489d7037fde" />
