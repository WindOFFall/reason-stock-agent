@echo off
set PROJECT=C:\Users\Qoo\Desktop\my_workspace\reason-stock-agent
set CONDA=C:\Users\Qoo\anaconda2025

start "Backend" cmd /k "call %CONDA%\Scripts\activate.bat ai_agent && cd /d %PROJECT%\web\backend && python api.py"
timeout /t 1 /nobreak >nul
start "Frontend" cmd /k "cd /d %PROJECT%\web\frontend && npm run dev"
