@echo off
schtasks /delete /tn "NEPSE_Daily_ETL"    /f >nul 2>&1
schtasks /delete /tn "NEPSE_Daily_Report" /f >nul 2>&1

schtasks /create /tn "NEPSE_Daily_ETL"    /tr "C:\Users\ASUS\.conda\envs\adpy\python.exe C:\Codes\final_etl\fetcher.py" /sc daily /st 10:50 /ru SYSTEM /f
schtasks /create /tn "NEPSE_Daily_Report" /tr "C:\Users\ASUS\.conda\envs\adpy\python.exe C:\Codes\final_etl\report.py"  /sc daily /st 08:09 /ru SYSTEM /f

schtasks /run /tn "NEPSE_Daily_Report"
echo Done. Wait 15 seconds then check scheduler.log
pause