@echo off
rem Creates %APPDATA%\postgresql\pgpass.conf so psycopg/psql/pgAdmin can log in without a password prompt.
rem The password is typed in (masked) when the script runs - it is never stored in this file.
setlocal DisableDelayedExpansion

set "PGPASS_DIR=%APPDATA%\postgresql"
set "PGPASS_FILE=%PGPASS_DIR%\pgpass.conf"

set "PG_HOST=localhost"
set "PG_PORT=5432"
set "PG_USER=postgres"
set /p "PG_HOST=Host [%PG_HOST%]: "
set /p "PG_PORT=Port [%PG_PORT%]: "
set /p "PG_USER=User [%PG_USER%]: "

rem Read the password masked via PowerShell; delayed expansion stays off here so a "!" in it survives.
set "PG_PASSWORD="
for /f "usebackq delims=" %%P in (`powershell -NoProfile -Command "$secure = Read-Host 'Password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringBSTR([Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))"`) do set "PG_PASSWORD=%%P"

if not defined PG_PASSWORD (
    echo No password entered - nothing written.
    exit /b 1
)

if exist "%PGPASS_FILE%" (
    choice /m "%PGPASS_FILE% already exists. Overwrite it"
    if errorlevel 2 (
        echo Left the existing file unchanged.
        exit /b 1
    )
)

if not exist "%PGPASS_DIR%" mkdir "%PGPASS_DIR%"

setlocal EnableDelayedExpansion
rem pgpass format requires "\" and ":" inside a field to be escaped with a backslash.
set "PG_PASSWORD=!PG_PASSWORD:\=\\!"
set "PG_PASSWORD=!PG_PASSWORD::=\:!"
> "%PGPASS_FILE%" echo(!PG_HOST!:!PG_PORT!:*:!PG_USER!:!PG_PASSWORD!
endlocal

echo Wrote %PGPASS_FILE%
endlocal
