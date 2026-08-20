# Week 3 - Real-time Streaming Pipeline

## 목표

Olist 주문 데이터를 실제 주문 이벤트처럼 재구성하여
Kafka → Spark Structured Streaming → Raw Parquet 흐름을 구현한다.

## Architecture

Olist Sample  
→ Python Event Producer  
→ Kafka (`order-events`)  
→ Spark Structured Streaming  
→ Raw Parquet

<img width="1091" height="219" alt="image" src="https://github.com/user-attachments/assets/647c6693-bd7b-436e-8725-8f624d1f14fe" />

<img width="847" height="882" alt="image" src="https://github.com/user-attachments/assets/510514e3-aca0-4e5c-87bd-af5f9aabd19e" />


<img width="1907" height="256" alt="image" src="https://github.com/user-attachments/assets/bad601ba-85f0-49fe-b505-7242fb9fd211" />

<img width="1874" height="924" alt="image" src="https://github.com/user-attachments/assets/c8546289-7fcc-403f-a9b3-b489d7037fde" />
