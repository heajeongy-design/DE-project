import pandas as pd

orders = pd.read_csv("data/olist_orders_dataset.csv")

print("=== 기본 정보 ===")
print("행/열:", orders.shape)
print()

print("=== 컬럼 ===")
print(orders.columns.tolist())
print()

print("=== 상위 5행 ===")
print(orders.head())
print()

print("=== 주문 상태 ===")
print(orders["order_status"].value_counts())
print()

print("=== 결측치 ===")
print(
    orders[
        [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]
    ].isnull().sum()
)

print()
print("=== order_id 확인 ===")
print("전체 행:", len(orders))
print("고유 order_id:", orders["order_id"].nunique())

print("\n=== ORDER ITEMS 확인 ===")

items = pd.read_csv("data/olist_order_items_dataset.csv")

print("행/열:", items.shape)
print("컬럼:", items.columns.tolist())
print(items.head())


print("\n=== PAYMENTS 확인 ===")

payments = pd.read_csv("data/olist_order_payments_dataset.csv")

print("행/열:", payments.shape)
print("컬럼:", payments.columns.tolist())
print(payments.head())

# 주문 10,000건 샘플링 - 상태별로 몇건을 가져올지 선택함_원본 분포를 그대로 재현하는 프로젝트가 아닌, 파이프라인의 처리 상황을 검증하는 프로젝트
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

    sample_size = min(
        count,
        len(status_df)
    )

    sampled = status_df.sample(
        n=sample_size,
        random_state=42
    )

    sample_list.append(sampled)

orders_sample = pd.concat(
    sample_list,
    ignore_index=True
)

# 샘플링된 주문 ID
sample_order_ids = orders_sample["order_id"]

# 해당 주문에 속한 상품/결제 데이터만 필터링
items_sample = items[
    items["order_id"].isin(sample_order_ids)
]

payments_sample = payments[
    payments["order_id"].isin(sample_order_ids)
]

print("\n=== SAMPLE DATA 확인 ===")
print("orders_sample:", orders_sample.shape)
print("items_sample:", items_sample.shape)
print("payments_sample:", payments_sample.shape)

print("\n=== 샘플 주문 상태 ===")
print(orders_sample["order_status"].value_counts())