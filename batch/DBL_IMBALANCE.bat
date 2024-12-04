@REM @echo off
@REM For /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c-%%a-%%b)
@REM For /f "tokens=1-2 delims=/:" %%a in ('time /t') do (set mytime=%%a:%%b)
@REM title DBL_IMBALANCE - %mytime%
@REM echo off
@REM mode con:cols=200 lines=36

@REM :: ======================================
@REM "C:\Users\TRAINEE\AppData\Local\Programs\Python\Python313\python.exe" "D:/python_r_DA/python/IMBALANCE/imbalance.py"
@REM TIMEOUT 100
@REM PAUSE


@echo off
cd /d D:/python_r_DA/python/IMBALANCE
python imbalance.py
pause