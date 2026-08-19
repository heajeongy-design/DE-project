
#전체 olist 데이터에서 프로젝트에 사용할 샘플 주문과 관련 item/payments 선정

import pandas as pd

orders = pd.read_csv("data/olist_orders_dataset.csv")
items = pd.read_csv("data/olist_order_items_dataset.csv")
payments = pd.read_csv("data/olist_order_payments_dataset.csv")

sample_counts = {
    "delivered": 8500,
    "shipped": 500,
    "canceled": 300,
    "unavailable": 300,
    "processing": 200,
    "invoiced": 190,
    "created": 5,
    "approved": 2
}

sample_list = []

for status, count in sample_counts.items():
    status_df = orders[
        orders["order_status"] == status
    ]

    sample_size = min(count, len(status_df))

    sampled = status_df.sample(
        n=sample_size,
        random_state=42
    )

    sample_list.append(sampled)

orders_sample = pd.concat(
    sample_list,
    ignore_index=True
)

sample_order_ids = orders_sample["order_id"]

items_sample = items[
    items["order_id"].isin(sample_order_ids)
]

payments_sample = payments[
    payments["order_id"].isin(sample_order_ids)
]

print("=== SAMPLE DATA ===")
print("orders:", orders_sample.shape)
print("items:", items_sample.shape)
print("payments:", payments_sample.shape)

print("\n=== ORDER STATUS ===")
print(orders_sample["order_status"].value_counts())
