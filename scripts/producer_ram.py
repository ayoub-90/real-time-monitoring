"""
producer_ram.py
---------------
Kafka Producer — RAM / Memory Metrics
Collecte les métriques mémoire toutes les secondes et les publie dans le topic Kafka 'ram-metrics'.
"""

import json
import time
import psutil
from datetime import datetime
from kafka import KafkaProducer

# ==============================
# Config
# ==============================

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'ram-metrics'
INTERVAL = 1  # secondes

# ==============================
# Kafka Producer Setup
# ==============================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"[RAM Producer] Connected to Kafka broker at {KAFKA_BROKER}")
print(f"[RAM Producer] Publishing to topic: {TOPIC}")
print("[RAM Producer] Press Ctrl+C to stop.\n")

# ==============================
# Main Loop
# ==============================

try:
    while True:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        vm = psutil.virtual_memory()
        swap = psutil.swap_memory()

        payload = {
            "timestamp": timestamp,
            # Virtual Memory
            "ram_percent": vm.percent,
            "ram_used_gb": round(vm.used / (1024 ** 3), 3),
            "ram_available_gb": round(vm.available / (1024 ** 3), 3),
            "ram_total_gb": round(vm.total / (1024 ** 3), 3),
            # Swap
            "swap_percent": swap.percent,
            "swap_used_gb": round(swap.used / (1024 ** 3), 3),
            "swap_total_gb": round(swap.total / (1024 ** 3), 3),
        }

        producer.send(TOPIC, value=payload)
        print(f"[RAM] {timestamp} | RAM: {vm.percent}% ({payload['ram_used_gb']} GB) | Swap: {swap.percent}%")

        time.sleep(INTERVAL)

except KeyboardInterrupt:
    print("\n[RAM Producer] Stopped by user.")
    producer.flush()
    producer.close()
