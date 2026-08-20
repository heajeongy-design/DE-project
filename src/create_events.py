import pandas as pd
import uuid

def create_order_events():

    orders = pd.read_csv(
        "data/processed/orders_sample.csv"
    )

    events = []

    for _, row in orders.iterrows():

        events.append({
            "event_id": str(uuid.uuid4()),
            "order_id": row["order_id"],
            "customer_id": row["customer_id"],
            "event_type": "ORDER_CREATED",
            "event_time": row["order_purchase_timestamp"],
            "order_status": "created"
        })

        if pd.notna(row["order_approved_at"]):
            events.append({
                "event_id": str(uuid.uuid4()),
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "event_type": "PAYMENT_APPROVED",
                "event_time": row["order_approved_at"],
                "order_status": "approved"
            })

        if pd.notna(row["order_delivered_carrier_date"]):
            events.append({
                "event_id": str(uuid.uuid4()),
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "event_type": "SHIPPED",
                "event_time": row["order_delivered_carrier_date"],
                "order_status": "shipped"
            })

        if pd.notna(row["order_delivered_customer_date"]):
            events.append({
                "event_id": str(uuid.uuid4()),
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "event_type": "DELIVERED",
                "event_time": row["order_delivered_customer_date"],
                "order_status": "delivered"
            })

    events_df = pd.DataFrame(events)

    events_df["event_time"] = pd.to_datetime(
        events_df["event_time"]
    )

    events_df = events_df.sort_values(
        "event_time"
    ).reset_index(drop=True)

    return events_df


if __name__ == "__main__":

    events_df = create_order_events()

    print("=== EVENT DATA ===")
    print("전체 이벤트 수:", len(events_df))

    print("\n=== EVENT TYPE ===")
    print(events_df["event_type"].value_counts())

    print("\n=== SAMPLE EVENTS ===")
    print(events_df.head(10))