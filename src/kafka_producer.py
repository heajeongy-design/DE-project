import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from create_events import create_order_events


KAFKA_BROKER = "localhost:9092"
TOPIC = "order-events"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})


def delivery_report(err, msg):
    if err is not None:
        print(f"전송 실패: {err}")
    else:
        print(
            f"전송 성공 | "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


events_df = create_order_events()

# 과제 테스트 단계에서는 전체 37,444건을 보내지 않고
# 먼저 20건만 전송해서 동작을 확인
test_events = events_df.head(20)

print(f"전송할 이벤트 수: {len(test_events)}")
print("Kafka Producer 시작\n")

for _, row in test_events.iterrows():

    event = {
        "event_id": row["event_id"],
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "event_type": row["event_type"],

        # Olist 원본에서 발생했던 시간
        "original_event_time": row["event_time"].isoformat(),

        # 실시간 시뮬레이션상 발생 시간
        "event_time": datetime.now(timezone.utc).isoformat(),

        # Kafka에 실제 넣는 시간
        "ingestion_time": datetime.now(timezone.utc).isoformat(),

        "order_status": row["order_status"]
    }

    producer.produce(
        TOPIC,
        key=event["order_id"],
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report
    )

    producer.poll(0)

    print(
        f'{event["event_type"]} | '
        f'{event["order_id"]}'
    )

    # 실시간으로 들어오는 것처럼 약간의 간격을 둠
    time.sleep(1)


producer.flush()

print("\nKafka Producer 종료")