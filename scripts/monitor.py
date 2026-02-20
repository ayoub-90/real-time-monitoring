import psutil
import time
import os
import pickle
import json
from datetime import datetime, timezone

import gspread
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ==============================
# Config
# ==============================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

SPREADSHEET_NAME = "System Monitor"
INTERVAL = 30  # secondes — 30s pour eviter le quota Google Sheets (60 req/min)

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC  = 'system-metrics'

BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(BASE_DIR, '..', 'config', 'credentials.json')
TOKEN_PATH       = os.path.join(BASE_DIR, '..', 'config', 'token.pickle')

# ==============================
# Authentification Google
# ==============================

def authenticate():
    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)

    return creds

# ==============================
# Connexion Kafka
# ==============================

def connect_kafka():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"[Kafka] Connecte au broker {KAFKA_BROKER} - topic: {KAFKA_TOPIC}")
        return producer
    except NoBrokersAvailable:
        print("[Kafka] Broker non disponible - donnees envoyees uniquement vers Google Sheets.")
        return None

# ==============================
# Collecte des metriques
# ==============================

def get_metrics(prev_disk=None, prev_net=None):
    # datetime.now(timezone.utc) remplace utcnow() qui est deprecated
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    cpu_percent  = psutil.cpu_percent(interval=1)
    cpu_per_core = psutil.cpu_percent(interval=0, percpu=True)

    vm           = psutil.virtual_memory()
    ram_percent  = vm.percent
    ram_used_gb  = round(vm.used / (1024 ** 3), 2)
    swap_percent = psutil.swap_memory().percent

    disk_percent = psutil.disk_usage('/').percent
    disk_io      = psutil.disk_io_counters()
    if prev_disk:
        disk_read_kb_s  = round((disk_io.read_bytes  - prev_disk.read_bytes)  / INTERVAL / 1024, 2)
        disk_write_kb_s = round((disk_io.write_bytes - prev_disk.write_bytes) / INTERVAL / 1024, 2)
    else:
        disk_read_kb_s = disk_write_kb_s = 0

    net_io = psutil.net_io_counters()
    if prev_net:
        net_sent_kb_s = round((net_io.bytes_sent - prev_net.bytes_sent) / INTERVAL / 1024, 2)
        net_recv_kb_s = round((net_io.bytes_recv - prev_net.bytes_recv) / INTERVAL / 1024, 2)
    else:
        net_sent_kb_s = net_recv_kb_s = 0

    payload = {
        "timestamp":       timestamp,
        "cpu_percent":     cpu_percent,
        "cpu_per_core":    cpu_per_core,
        "ram_percent":     ram_percent,
        "ram_used_gb":     ram_used_gb,
        "swap_percent":    swap_percent,
        "disk_percent":    disk_percent,
        "disk_read_kb_s":  disk_read_kb_s,
        "disk_write_kb_s": disk_write_kb_s,
        "net_sent_kb_s":   net_sent_kb_s,
        "net_recv_kb_s":   net_recv_kb_s,
    }

    row = [
        timestamp, cpu_percent, str(cpu_per_core),
        ram_percent, ram_used_gb, swap_percent,
        disk_percent, disk_read_kb_s, disk_write_kb_s,
        net_sent_kb_s, net_recv_kb_s
    ]

    return payload, row, disk_io, net_io

# ==============================
# Programme principal
# ==============================

def main():
    print("Authentification Google Sheets...")
    creds  = authenticate()
    client = gspread.authorize(creds)
    print("Google Sheets : connecte !")

    try:
        sheet = client.open(SPREADSHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sheet = client.create(SPREADSHEET_NAME)
        print(f"Spreadsheet '{SPREADSHEET_NAME}' cree.")

    # Onglet historique
    try:
        ts_sheet = sheet.worksheet("TimeSeries Data")
    except gspread.WorksheetNotFound:
        ts_sheet = sheet.add_worksheet(title="TimeSeries Data", rows=5000, cols=20)
        ts_sheet.append_row([
            "Timestamp", "CPU%", "CPU per Core", "RAM%", "RAM Used GB",
            "Swap%", "Disk%", "Disk Read KB/s", "Disk Write KB/s",
            "Net Sent KB/s", "Net Recv KB/s"
        ])
        print("Onglet 'TimeSeries Data' cree.")

    # Onglet temps reel
    try:
        last_sheet = sheet.worksheet("Last Only")
    except gspread.WorksheetNotFound:
        last_sheet = sheet.add_worksheet(title="Last Only", rows=20, cols=5)
        print("Onglet 'Last Only' cree.")

    # Kafka
    producer = connect_kafka()
    if producer:
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            if KAFKA_TOPIC not in admin.list_topics():
                admin.create_topics([NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)])
                print(f"[Kafka] Topic '{KAFKA_TOPIC}' cree.")
            admin.close()
        except Exception:
            pass

    prev_disk = None
    prev_net  = None

    print(f"\nMonitoring demarre - envoi toutes les {INTERVAL}s. Ctrl+C pour arreter.\n")

    while True:
        try:
            payload, row, prev_disk, prev_net = get_metrics(prev_disk, prev_net)

            # 1. Kafka
            if producer:
                try:
                    producer.send(KAFKA_TOPIC, value=payload)
                    kafka_status = "OK"
                except Exception as e:
                    kafka_status = f"ERREUR ({e})"
            else:
                kafka_status = "NON CONNECTE"

            # 2. Historique — 1 seul appel API
            ts_sheet.append_row(row)

            # 3. Last Only — 1 seul appel API (update au lieu de 11 x append_row)
            last_data = [
                ["Metric",          "Value"],
                ["Timestamp",       payload["timestamp"]],
                ["CPU%",            payload["cpu_percent"]],
                ["CPU per Core",    str(payload["cpu_per_core"])],
                ["RAM%",            payload["ram_percent"]],
                ["RAM Used GB",     payload["ram_used_gb"]],
                ["Swap%",           payload["swap_percent"]],
                ["Disk%",           payload["disk_percent"]],
                ["Disk Read KB/s",  payload["disk_read_kb_s"]],
                ["Disk Write KB/s", payload["disk_write_kb_s"]],
                ["Net Sent KB/s",   payload["net_sent_kb_s"]],
                ["Net Recv KB/s",   payload["net_recv_kb_s"]],
            ]
            last_sheet.update(range_name="A1", values=last_data)  # 1 seul appel au lieu de 13 !

            print(
                f"[{payload['timestamp']}] "
                f"CPU: {payload['cpu_percent']}% | "
                f"RAM: {payload['ram_percent']}% | "
                f"Disk: {payload['disk_percent']}% | "
                f"Kafka: {kafka_status}"
            )

            time.sleep(INTERVAL)

        except KeyboardInterrupt:
            print("\nMonitoring arrete.")
            if producer:
                producer.flush()
                producer.close()
            break

        except Exception as e:
            print(f"Erreur : {e} - nouvelle tentative dans {INTERVAL}s...")
            time.sleep(INTERVAL)


if __name__ == "__main__":
    main()