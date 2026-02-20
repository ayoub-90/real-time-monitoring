"""
producer_disk.py
----------------
Kafka Producer — Disk Metrics
Collecte les métriques disque toutes les secondes et les publie dans le topic Kafka 'disk-metrics'.
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
TOPIC = 'disk-metrics'
INTERVAL = 1  # secondes

# ==============================
# Kafka Producer Setup
# ==============================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"[Disk Producer] Connected to Kafka broker at {KAFKA_BROKER}")
print(f"[Disk Producer] Publishing to topic: {TOPIC}")
print("[Disk Producer] Press Ctrl+C to stop.\n")

# ==============================
# Main Loop
# ==============================

prev_disk_io = psutil.disk_io_counters()

try:
    while True:
        time.sleep(INTERVAL)

        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        disk_usage = psutil.disk_usage('/')
        disk_io = psutil.disk_io_counters()

        # Delta per second
        read_kb_s = (disk_io.read_bytes - prev_disk_io.read_bytes) / 1024 / INTERVAL
        write_kb_s = (disk_io.write_bytes - prev_disk_io.write_bytes) / 1024 / INTERVAL
        read_ops_s = (disk_io.read_count - prev_disk_io.read_count) / INTERVAL
        write_ops_s = (disk_io.write_count - prev_disk_io.write_count) / INTERVAL

        prev_disk_io = disk_io

        payload = {
            "timestamp": timestamp,
            # Usage
            "disk_percent": disk_usage.percent,
            "disk_used_gb": round(disk_usage.used / (1024 ** 3), 2),
            "disk_free_gb": round(disk_usage.free / (1024 ** 3), 2),
            "disk_total_gb": round(disk_usage.total / (1024 ** 3), 2),
            # Throughput
            "read_kb_s": round(read_kb_s, 2),
            "write_kb_s": round(write_kb_s, 2),
            "read_ops_s": round(read_ops_s, 2),
            "write_ops_s": round(write_ops_s, 2),
        }

        producer.send(TOPIC, value=payload)
        print(f"[DISK] {timestamp} | Usage: {disk_usage.percent}% | R: {payload['read_kb_s']} KB/s | W: {payload['write_kb_s']} KB/s")

except KeyboardInterrupt:
    print("\n[Disk Producer] Stopped by user.")
    producer.flush()
    producer.close()
