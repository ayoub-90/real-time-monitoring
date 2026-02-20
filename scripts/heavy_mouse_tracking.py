import time
import os
import pickle
import math
from datetime import datetime
from pynput import mouse

import gspread
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ==============================
# Google API Config
# ==============================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

SPREADSHEET_NAME = "Mouse Tracker"

# ==============================
# Tracking Config
# ==============================

TRACK_INTERVAL = 0.1
PRINT_INTERVAL = 1

# ==============================
# Global Variables
# ==============================

current_position = (0, 0)
click_events = []

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
# Mouse Listener
# ==============================

def on_move(x, y):
    global current_position
    current_position = (x, y)

def on_click(x, y, button, pressed):
    global click_events
    if pressed:
        click_events.append({
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "x": x,
            "y": y,
            "button": button.name
        })

listener = mouse.Listener(on_move=on_move, on_click=on_click)
listener.start()

# ==============================
# Main Tracking Function
# ==============================

def track_mouse():

    global current_position, click_events

    # Google Sheets connection
    creds = authenticate()
    client = gspread.authorize(creds)

    try:
        sheet = client.open(SPREADSHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sheet = client.create(SPREADSHEET_NAME)

    # TimeSeries sheet (historical)
    try:
        ts_sheet = sheet.worksheet("Mouse_TimeSeries")
    except:
        ts_sheet = sheet.add_worksheet(title="Mouse_TimeSeries", rows=1000, cols=10)
        ts_sheet.append_row([
            "Timestamp",
            "Mouse_X",
            "Mouse_Y",
            "Speed_px_per_sec",
            "Clicks_Count",
            "Click_Details"
        ])

    # LastOnly sheet (real-time)
    try:
        last_sheet = sheet.worksheet("Mouse_LastOnly")
    except:
        last_sheet = sheet.add_worksheet(title="Mouse_LastOnly", rows=20, cols=10)
        last_sheet.append_row(["Metric", "Value"])

    prev_position = current_position
    last_print_time = time.time()

    try:
        while True:
            loop_start = time.time()

            x, y = current_position
            px, py = prev_position

            distance = math.sqrt((x - px)**2 + (y - py)**2)
            speed = distance / TRACK_INTERVAL

            prev_position = current_position

            if time.time() - last_print_time >= PRINT_INTERVAL:

                timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                # Click summary
                num_clicks = len(click_events)
                click_details = "; ".join([
                    f"{c['button']}({c['x']},{c['y']})"
                    for c in click_events
                ])

                # =======================
                # Console Print
                # =======================
                print("\n==============================")
                print("Timestamp:", timestamp)
                print("Current Position:", (x, y))
                print("Speed (px/sec):", round(speed, 2))
                print("Clicks in last 1s:", num_clicks)

                # =======================
                # Send to TimeSeries
                # =======================
                ts_sheet.append_row([
                    timestamp,
                    x,
                    y,
                    round(speed, 2),
                    num_clicks,
                    click_details
                ])

                # =======================
                # Update LastOnly sheet
                # =======================
                last_sheet.clear()
                last_sheet.append_row(["Metric", "Value"])
                last_sheet.append_row(["Timestamp", timestamp])
                last_sheet.append_row(["Mouse_X", x])
                last_sheet.append_row(["Mouse_Y", y])
                last_sheet.append_row(["Speed_px_per_sec", round(speed, 2)])
                last_sheet.append_row(["Clicks_Count", num_clicks])
                last_sheet.append_row(["Click_Details", click_details])

                # Reset clicks after sending
                click_events = []
                last_print_time = time.time()

            elapsed = time.time() - loop_start
            time.sleep(max(0, TRACK_INTERVAL - elapsed))

    except KeyboardInterrupt:
        print("\nTracking stopped by user.")

# ==============================
# Run
# ==============================

if __name__ == "__main__":
    track_mouse()
