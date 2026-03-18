@echo off
REM Complete Data Pipeline Runner (Windows)
REM Cleans raw data and loads into PostgreSQL, MongoDB, and Neo4j

echo BIG DATA STORAGE & RETRIEVAL - COMPLETE PIPELINE
echo ==================================================
echo This script will:
echo   1. Clean all raw data files
echo   2. Load cleaned data into PostgreSQL
echo   3. Load cleaned data into MongoDB
echo   4. Load cleaned data into Neo4j
echo ==================================================

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed
    pause
    exit /b 1
)

REM Change to project root directory
cd /d "%~dp0\.."

REM Run the complete pipeline
python scripts\run_complete_pipeline.py

REM Check exit status
if errorlevel 1 (
    echo.
    echo ==================================================
    echo PIPELINE FAILED!
    echo Check error messages above for details.
    echo ==================================================
    pause
    exit /b 1
) else (
    echo.
    echo ==================================================
    echo COMPLETE PIPELINE EXECUTED SUCCESSFULLY!
    echo All data cleaned and loaded into all three databases!
    echo ==================================================
    pause
)
