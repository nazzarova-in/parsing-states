@echo off
cd /d "C:\Users\Lena Nazarova\Desktop\MontanaParsing"

echo [STEP 1] Start Docker...
docker-compose up -d

echo [STEP 2] Waiting 20 seconds...
timeout /t 20 >nul

echo [STEP 3] Running a Python Script...
venv\Scripts\python.exe main.py

echo [DONE] The script is complete.
