@echo off
chcp 65001 > nul
echo ========================================
echo   홍군 퀀트 v7.4.2 Core 엔진 실행기
echo ========================================
echo.

if not exist core_engine.py (
    echo [오류] core_engine.py 파일이 같은 폴더에 없습니다!
    pause
    exit
)

python core_engine.py

echo.
echo ========================================
echo   프로그램이 종료되었습니다.
echo ========================================
pause