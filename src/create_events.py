import pandas as pd
import uuid
from datetime import datetime

orders = pd.read_csv(
    "data/processed/orders_sample.csv"
)

events = []

for _, row in orders.iterrows():

    # 1. 주문 생성
    events.append({
        "event_id": str(uuid.uuid4()),
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "event_type": "ORDER_CREATED",
        "event_time": row["order_purchase_timestamp"],
        "order_status": "created",
        "ingestion_time": datetime.now().isoformat()
    })

    # 2. 결제 승인
    if pd.notna(row["order_approved_at"]):
        events.append({
            "event_id": str(uuid.uuid4()),
            "order_id": row["order_id"],
            "customer_id": row["customer_id"],
            "event_type": "PAYMENT_APPROVED",
            "event_time": row["order_approved_at"],
            "order_status": "approved",
            "ingestion_time": datetime.now().isoformat()
        })

    # 3. 배송사 전달
    if pd.notna(row["order_delivered_carrier_date"]):
        events.append({
            "event_id": str(uuid.uuid4()),
            "order_id": row["order_id"],
            "customer_id": row["customer_id"],
            "event_type": "SHIPPED",
            "event_time": row["order_delivered_carrier_date"],
            "order_status": "shipped",
            "ingestion_time": datetime.now().isoformat()
        })

    # 4. 배송 완료
    if pd.notna(row["order_delivered_customer_date"]):
        events.append({
            "event_id": str(uuid.uuid4()),
            "order_id": row["order_id"],
            "customer_id": row["customer_id"],
            "event_type": "DELIVERED",
            "event_time": row["order_delivered_customer_date"],
            "order_status": "delivered",
            "ingestion_time": datetime.now().isoformat()
        })

events_df = pd.DataFrame(events)

print("=== EVENT DATA ===")
print("전체 이벤트 수:", len(events_df))
print()

print("=== EVENT TYPE ===")
print(events_df["event_type"].value_counts())
print()

print("=== SAMPLE EVENTS ===")
print(events_df.head(10))