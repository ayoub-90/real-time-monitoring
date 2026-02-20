"""
producer_cpu.py
---------------
Kafka Producer — CPU Metrics
Collecte les métriques CPU toutes les secondes et les publie dans le topic Kafka 'cpu-metrics'.
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
TOPIC = 'cpu-metrics'
INTERVAL = 1  # secondes

# ==============================
# Kafka Producer Setup
# ==============================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"[CPU Producer] Connected to Kafka broker at {KAFKA_BROKER}")
print(f"[CPU Producer] Publishing to topic: {TOPIC}")
print("[CPU Producer] Press Ctrl+C to stop.\n")

# ==============================
# Main Loop
# ==============================

try:
    while True:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        cpu_global = psutil.cpu_percent(interval=0)
        cpu_per_core = psutil.cpu_percent(interval=0, percpu=True)
        cpu_freq = psutil.cpu_freq()
        cpu_count = psutil.cpu_count(logical=True)

        # CPU Temperature (Linux only — may not be available on all machines)
        try:
            temps = psutil.sensors_temperatures()
            cpu_temp = temps.get('coretemp', [{}])[0].current if temps else None
        except Exception:
            cpu_temp = None

        payload = {
            "timestamp": timestamp,
            "cpu_percent": cpu_global,
            "cpu_per_core": cpu_per_core,
            "cpu_count": cpu_count,
            "cpu_freq_mhz": round(cpu_freq.current, 2) if cpu_freq else None,
            "cpu_temp_celsius": cpu_temp
        }

        producer.send(TOPIC, value=payload)
        print(f"[CPU] {timestamp} | CPU: {cpu_global}% | Freq: {payload['cpu_freq_mhz']} MHz")

        time.sleep(INTERVAL)

except KeyboardInterrupt:
    print("\n[CPU Producer] Stopped by user.")
    producer.flush()
    producer.close()
