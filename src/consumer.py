from confluent_kafka import Consumer, KafkaException
import requests
import json
import time
import base64

# ===============================
# Kafka configuration
# ===============================
conf = {
    "bootstrap.servers": "ip:9092",
    "group.id": "dhis-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,

    #  SASL Auth
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "sasluser",
    "sasl.password": "saslpass",
   # Heartbeats & session:
    "session.timeout.ms": 60000,      # give more time before consumer is considered dead
    "heartbeat.interval.ms": 20000,   # should be smaller than session timeout

    # Poll tuning:
    "max.poll.interval.ms": 60000,    # enough time for your processing
    #"max.poll.records": 1,            # short processing per poll = more frequent poll()
    "queued.min.messages": 1,
    # other optional stability configs


}

consumer = Consumer(conf)
topic = "dhis-topic"
consumer.subscribe([topic])

# ===============================
# DHIS configuration
# ===============================
DHIS_URL = "https://baseurl/api/dataValueSets?async=true"
USERNAME = "username"
PASSWORD = "pass"

basic_auth = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()

headers = {
    "Authorization": f"Basic {basic_auth}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

print("Kafka consumer started. Waiting for messages...\n")


# ===============================
# Send to DHIS function
# ===============================
def send_to_dhis(payload: str) -> bool:
    if not payload.strip():
        print("Empty payload, skipping")
        return True  # safe to commit

    try:
        response = requests.post(
            DHIS_URL,
            headers=headers,
            data=payload,
            timeout=60
        )

        if response.status_code in (200, 201):
            print("DHIS accepted payload")
            return True
        else:
            print(f"DHIS rejected payload [{response.status_code}]")
            print(response.text)
            return False

    except Exception as ex:
        print(f"DHIS request failed: {ex}")
        return False
# ===============================
# Consume loop
# ===============================
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue  # normal - reached end of partition
            else:
                print(f"Consumer error: {msg.error()}")
                continue   # safer than raising in production

        # =============================
        # Handle NULL message value
        # =============================
        value = msg.value()
        if value is None:
            print("Skipping message with NULL value (tombstone or empty payload)\n")
            continue

        payload = value.decode("utf-8")

        # ===========================
        #  Extract Kafka Headers
        # ===========================
        headers_dict = {}
        if msg.headers():
            for header_key, header_value in msg.headers():
                if header_value is not None:
                    headers_dict[header_key] = header_value.decode("utf-8")
                else:
                    headers_dict[header_key] = None

        print("\n--- Message Headers ---")
        for k, v in headers_dict.items():
            print(f"{k}: {v}")
        print("------------------------\n")

        success = send_to_dhis(payload)

        if success:
            consumer.commit(msg)
            print("Offset committed\n")
        else:
            print("Retrying in 5 seconds...\n")
            time.sleep(5)

except KeyboardInterrupt:
    print("\nConsumer stopped by user")

finally:
    consumer.close()




