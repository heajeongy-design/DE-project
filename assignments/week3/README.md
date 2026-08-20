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
