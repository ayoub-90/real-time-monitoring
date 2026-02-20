import time
import os
import pickle
from datetime import datetime
import statistics
import psutil

import gspread
from google_auth_oauthlib.flow import InstalledAppFlow

# ==============================
# Google API Config
# ==============================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

SPREADSHEET_NAME = "Network Activity Monitor"

# ==============================
# Tracking Config
# ==============================

TRACK_INTERVAL = 0.1
AGGREGATION_INTERVAL = 1

# ==============================
# Google Authentication
# ==============================

def authenticate():
    creds = None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)

        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds

# ==============================
# Main Monitoring Function
# ==============================

def monitor_network():

    creds = authenticate()
    client = gspread.authorize(creds)

    try:
        sheet = client.open(SPREADSHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sheet = client.create(SPREADSHEET_NAME)

    # TimeSeries Sheet
    try:
        ts_sheet = sheet.worksheet("Network_TimeSeries")
    except:
        ts_sheet = sheet.add_worksheet(title="Network_TimeSeries", rows=1000, cols=10)
        ts_sheet.append_row([
            "Timestamp",
            "Upload_KB_s",
            "Download_KB_s",
            "Packets_Out_s",
            "Packets_In_s",
            "Errors_Total",
            "Drops_Total"
        ])

    # LastOnly Sheet
    try:
        last_sheet = sheet.worksheet("Network_LastOnly")
    except:
        last_sheet = sheet.add_worksheet(title="Network_LastOnly", rows=20, cols=10)
        last_sheet.append_row(["Metric", "Value"])

    prev_counters = psutil.net_io_counters()

    upload_samples = []
    download_samples = []
    packets_out_samples = []
    packets_in_samples = []

    last_aggregation_time = time.time()

    try:
        while True:
            loop_start = time.time()

            current = psutil.net_io_counters()

            # -------- Delta calculation --------
            delta_bytes_sent = current.bytes_sent - prev_counters.bytes_sent
            delta_bytes_recv = current.bytes_recv - prev_counters.bytes_recv

            delta_packets_sent = current.packets_sent - prev_counters.packets_sent
            delta_packets_recv = current.packets_recv - prev_counters.packets_recv

            prev_counters = current

            # Convert to per-second rate
            upload_kb_s = (delta_bytes_sent / 1024) / TRACK_INTERVAL
            download_kb_s = (delta_bytes_recv / 1024) / TRACK_INTERVAL

            packets_out_s = delta_packets_sent / TRACK_INTERVAL
            packets_in_s = delta_packets_recv / TRACK_INTERVAL

            upload_samples.append(upload_kb_s)
            download_samples.append(download_kb_s)
            packets_out_samples.append(packets_out_s)
            packets_in_samples.append(packets_in_s)

            # -------- Aggregate every 1 second --------
            if time.time() - last_aggregation_time >= AGGREGATION_INTERVAL:

                timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                upload_avg = round(statistics.mean(upload_samples), 2)
                download_avg = round(statistics.mean(download_samples), 2)
                packets_out_avg = round(statistics.mean(packets_out_samples), 2)
                packets_in_avg = round(statistics.mean(packets_in_samples), 2)

                errors_total = current.errin + current.errout
                drops_total = current.dropin + current.dropout

                # Console Print
                print("\n==============================")
                print("Timestamp:", timestamp)
                print("Upload KB/s:", upload_avg)
                print("Download KB/s:", download_avg)
                print("Packets Out/s:", packets_out_avg)
                print("Packets In/s:", packets_in_avg)

                # -------- Send TimeSeries (averaged) --------
                ts_sheet.append_row([
                    timestamp,
                    upload_avg,
                    download_avg,
                    packets_out_avg,
                    packets_in_avg,
                    errors_total,
                    drops_total
                ])

                # -------- Update LastOnly (latest raw) --------
                last_sheet.clear()
                last_sheet.append_row(["Metric", "Value"])
                last_sheet.append_row(["Timestamp", timestamp])
                last_sheet.append_row(["Upload_KB_s", round(upload_kb_s, 2)])
                last_sheet.append_row(["Download_KB_s", round(download_kb_s, 2)])
                last_sheet.append_row(["Packets_Out_s", round(packets_out_s, 2)])
                last_sheet.append_row(["Packets_In_s", round(packets_in_s, 2)])
                last_sheet.append_row(["Errors_Total", errors_total])
                last_sheet.append_row(["Drops_Total", drops_total])

                # Reset samples
                upload_samples = []
                download_samples = []
                packets_out_samples = []
                packets_in_samples = []

                last_aggregation_time = time.time()

            elapsed = time.time() - loop_start
            time.sleep(max(0, TRACK_INTERVAL - elapsed))

    except KeyboardInterrupt:
        print("\nNetwork monitoring stopped.")

# ==============================
# Run
# ==============================

if __name__ == "__main__":
    monitor_network()
