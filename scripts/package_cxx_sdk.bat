@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "POWERSHELL_EXE=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "VSWHERE_EXE=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe"
set "VCVARS_BAT="
set "VS_INSTALL_DIR="

if exist "%VSWHERE_EXE%" (
    for /f "usebackq delims=" %%I in (`"%VSWHERE_EXE%" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath`) do set "VS_INSTALL_DIR=%%I"
)

if defined VS_INSTALL_DIR if exist "%VS_INSTALL_DIR%\VC\Auxiliary\Build\vcvars64.bat" (
    set "VCVARS_BAT=%VS_INSTALL_DIR%\VC\Auxiliary\Build\vcvars64.bat"
)

if not defined VCVARS_BAT (
    echo Failed to locate vcvars64.bat 1>&2
    endlocal & exit /b 1
)

call "%VCVARS_BAT%" >nul
if errorlevel 1 (
    endlocal & exit /b %errorlevel%
)

"%POWERSHELL_EXE%" -ExecutionPolicy Bypass -File "%SCRIPT_DIR%package_cxx_sdk.ps1" %*
set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%