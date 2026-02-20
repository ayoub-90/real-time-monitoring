@echo off
echo Creating Monitoring System Project Structure...


:: Main folders
mkdir scripts
mkdir config
mkdir diagrams

:: Create empty python files
type nul > scripts\simple_monitor.py
type nul > scripts\producer_cpu.py
type nul > scripts\producer_ram.py
type nul > scripts\producer_disk.py
type nul > scripts\consumer.py

:: Create config placeholder
type nul > config\credentials.json

:: Create requirements.txt
(
echo psutil
echo gspread
echo google-auth
echo google-auth-oauthlib
echo google-auth-httplib2
echo kafka-python
) > requirements.txt

:: Create README.md
(
echo # Monitoring System - Real Time Data Pipeline
echo.
echo ## Description
echo Real-time monitoring system using Python, Google Sheets and Looker Studio.
echo.
echo ## Architecture
echo Machine ^> Python Monitoring ^> Google Sheets ^> Looker Studio
echo.
echo ## Optional Advanced Architecture
echo Producers ^> Kafka ^> Consumer ^> Google Sheets ^> Looker Studio
echo.
echo ## Tech Stack
echo - Python
echo - Kafka
echo - Google Sheets API
echo - Looker Studio
) > README.md

:: Create .gitignore
(
echo __pycache__/
echo *.pyc
echo config/credentials.json
echo .env
) > .gitignore

echo.
echo Project structure created successfully!
pause
