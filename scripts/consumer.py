"""
consumer.py
-----------
Kafka Consumer — Multi-Topic System Metrics
Consomme les messages des topics 'cpu-metrics', 'ram-metrics', 'disk-metrics'
et écrit les données en temps réel dans un Google Sheet dédié.
"""

import json
import os
import pickle
import threading
from datetime import datetime
from collections import defaultdict

from kafka import KafkaConsumer
import gspread
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ==============================
# Config
# ==============================

KAFKA_BROKER = 'localhost:9092'
TOPICS = ['cpu-metrics', 'ram-metrics', 'disk-metrics']
SPREADSHEET_NAME = "Kafka System Monitor"

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# ==============================
# Google Authentication
# ==============================

def authenticate():
    creds = None
    token_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'token.pickle')
    creds_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'credentials.json')

    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return creds

# ==============================
# Google Sheets Setup
# ==============================

def setup_sheets(client):
    try:
        sheet = client.open(SPREADSHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sheet = client.create(SPREADSHEET_NAME)
        print(f"[Consumer] Created new spreadsheet: {SPREADSHEET_NAME}")

    sheets = {}

    # --- CPU Sheet ---
    try:
        sheets['cpu'] = sheet.worksheet("CPU_Kafka")
    except:
        ws = sheet.add_worksheet(title="CPU_Kafka", rows=5000, cols=10)
        ws.append_row(["Timestamp", "CPU%", "CPU_Per_Core", "CPU_Count", "Freq_MHz", "Temp_C"])
        sheets['cpu'] = ws

    # --- RAM Sheet ---
    try:
        sheets['ram'] = sheet.worksheet("RAM_Kafka")
    except:
        ws = sheet.add_worksheet(title="RAM_Kafka", rows=5000, cols=10)
        ws.append_row(["Timestamp", "RAM%", "RAM_Used_GB", "RAM_Available_GB", "RAM_Total_GB", "Swap%", "Swap_Used_GB"])
        sheets['ram'] = ws

    # --- Disk Sheet ---
    try:
        sheets['disk'] = sheet.worksheet("Disk_Kafka")
    except:
        ws = sheet.add_worksheet(title="Disk_Kafka", rows=5000, cols=10)
        ws.append_row(["Timestamp", "Disk%", "Disk_Used_GB", "Disk_Free_GB", "Disk_Total_GB", "Read_KB_s", "Write_KB_s", "Read_Ops_s", "Write_Ops_s"])
        sheets['disk'] = ws

    # --- LastOnly Sheet ---
    try:
        sheets['last'] = sheet.worksheet("LastOnly_Kafka")
    except:
        ws = sheet.add_worksheet(title="LastOnly_Kafka", rows=30, cols=10)
        ws.append_row(["Metric", "Value", "Updated_At"])
        sheets['last'] = ws

    return sheets

# ==============================
# Row Builders
# ==============================

def build_cpu_row(data):
    return [
        data.get("timestamp"),
        data.get("cpu_percent"),
        str(data.get("cpu_per_core", [])),
        data.get("cpu_count"),
        data.get("cpu_freq_mhz"),
        data.get("cpu_temp_celsius", "N/A")
    ]

def build_ram_row(data):
    return [
        data.get("timestamp"),
        data.get("ram_percent"),
        data.get("ram_used_gb"),
        data.get("ram_available_gb"),
        data.get("ram_total_gb"),
        data.get("swap_percent"),
        data.get("swap_used_gb"),
    ]

def build_disk_row(data):
    return [
        data.get("timestamp"),
        data.get("disk_percent"),
        data.get("disk_used_gb"),
        data.get("disk_free_gb"),
        data.get("disk_total_gb"),
        data.get("read_kb_s"),
        data.get("write_kb_s"),
        data.get("read_ops_s"),
        data.get("write_ops_s"),
    ]

# ==============================
# Update LastOnly Sheet
# ==============================

# In-memory snapshot of latest values per topic
latest = defaultdict(dict)

def update_last_only(last_sheet, topic, data):
    latest[topic].update(data)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    rows = [["Metric", "Value", "Updated_At"]]

    # CPU
    if 'cpu-metrics' in latest:
        d = latest['cpu-metrics']
        rows.append(["[CPU] cpu_percent", d.get("cpu_percent", ""), now])
        rows.append(["[CPU] cpu_freq_mhz", d.get("cpu_freq_mhz", ""), now])
        rows.append(["[CPU] cpu_temp_celsius", d.get("cpu_temp_celsius", "N/A"), now])

    # RAM
    if 'ram-metrics' in latest:
        d = latest['ram-metrics']
        rows.append(["[RAM] ram_percent", d.get("ram_percent", ""), now])
        rows.append(["[RAM] ram_used_gb", d.get("ram_used_gb", ""), now])
        rows.append(["[RAM] swap_percent", d.get("swap_percent", ""), now])

    # Disk
    if 'disk-metrics' in latest:
        d = latest['disk-metrics']
        rows.append(["[DISK] disk_percent", d.get("disk_percent", ""), now])
        rows.append(["[DISK] read_kb_s", d.get("read_kb_s", ""), now])
        rows.append(["[DISK] write_kb_s", d.get("write_kb_s", ""), now])

    last_sheet.clear()
    last_sheet.update(f"A1:C{len(rows)}", rows)

# ==============================
# Main Consumer Loop
# ==============================

def main():
    print("[Consumer] Authenticating with Google...")
    creds = authenticate()
    client = gspread.authorize(creds)

    print("[Consumer] Setting up Google Sheets...")
    sheets = setup_sheets(client)

    print(f"[Consumer] Connecting to Kafka broker at {KAFKA_BROKER}")
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='system-monitor-consumer'
    )

    print(f"[Consumer] Listening to topics: {TOPICS}")
    print("[Consumer] Press Ctrl+C to stop.\n")

    msg_count = 0

    try:
        for message in consumer:
            topic = message.topic
            data = message.value

            print(f"[{topic}] {data.get('timestamp')} | {json.dumps({k: v for k, v in data.items() if k != 'cpu_per_core'})}")

            if topic == 'cpu-metrics':
                sheets['cpu'].append_row(build_cpu_row(data))
            elif topic == 'ram-metrics':
                sheets['ram'].append_row(build_ram_row(data))
            elif topic == 'disk-metrics':
                sheets['disk'].append_row(build_disk_row(data))

            # Update LastOnly every 5 messages to avoid quota issues
            msg_count += 1
            if msg_count % 5 == 0:
                update_last_only(sheets['last'], topic, data)

    except KeyboardInterrupt:
        print("\n[Consumer] Stopped by user.")
        consumer.close()

if __name__ == "__main__":
    main()
