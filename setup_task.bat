@echo off
:: ============================================================
:: NEPSE Scheduler — Windows Task Scheduler Setup
:: Run this file ONCE as Administrator.
:: ============================================================

echo.
echo  NEPSE Scheduler Setup
echo  =====================

:: Hardcoded conda env Python — works under SYSTEM account
set PYTHON_PATH=C:\Users\ASUS\.conda\envs\adpy\python.exe

if not exist "%PYTHON_PATH%" (
    echo  ERROR: Python not found at %PYTHON_PATH%
    echo  Check your conda env path and update this bat file.
    pause
    exit /b 1
)

:: Scripts live in the same folder as this batch file
set SCRIPT_PATH=%~dp0fetcher.py
set REPORT_PATH=%~dp0report.py
set TASK_NAME=NEPSE_Daily_ETL
set REPORT_TASK=NEPSE_Daily_Report

echo  Python       : %PYTHON_PATH%
echo  Fetcher      : %SCRIPT_PATH%
echo  Report       : %REPORT_PATH%
echo  Tasks        : %TASK_NAME%  /  %REPORT_TASK%
echo.

:: Delete existing tasks (clean re-register)
schtasks /delete /tn "%TASK_NAME%"   /f >nul 2>&1
schtasks /delete /tn "%REPORT_TASK%" /f >nul 2>&1

:: ── Task 1: fetcher.py at 10:50 AM ───────────────────────────────────────────
schtasks /create /tn "%TASK_NAME%" /tr "\"%PYTHON_PATH%\" \"%SCRIPT_PATH%\"" /sc daily /st 10:50 /ru SYSTEM /rl highest /f

if %errorlevel% neq 0 (
    echo  ERROR: Failed to register %TASK_NAME%.
    pause
    exit /b 1
)
echo  Task 1 registered: %TASK_NAME% at 10:50 AM

:: ── Task 2: report.py at 4:02 PM ─────────────────────────────────────────────
schtasks /create /tn "%REPORT_TASK%" /tr "\"%PYTHON_PATH%\" \"%REPORT_PATH%\"" /sc daily /st 16:02 /ru SYSTEM /rl highest /f

if %errorlevel% neq 0 (
    echo  ERROR: Failed to register %REPORT_TASK%.
    pause
    exit /b 1
)
echo  Task 2 registered: %REPORT_TASK% at 04:02 PM

echo.
echo  Both tasks registered successfully!
echo.
echo  [10:50 AM]  fetcher.py  — polls market, stops at 3:00 PM
echo  [ 4:02 PM]  report.py   — transforms data, builds PDF, sends notification
echo.
echo  Data is saved to:    %~dp0nepse_data\live_feed.csv
echo  Reports saved to:    %~dp0nepse_data\reports\
echo  Logs are saved to:   %~dp0nepse_data\scheduler.log
echo.
echo  To test the report generator RIGHT NOW, press any key.
echo  To skip the test, close this window.
pause
schtasks /run /tn "%REPORT_TASK%"
echo.
echo  Report task triggered. Check nepse_data\reports\ in a few seconds.

echo.
pause