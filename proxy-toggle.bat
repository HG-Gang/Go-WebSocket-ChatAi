@echo off
chcp 65001 >nul
REM TozoAI OpenAI 代理切换脚本。
REM 用法：
REM   proxy-toggle.bat on 73   使用 192.168.0.73:6478
REM   proxy-toggle.bat on 74   使用 192.168.0.74:6478
REM   proxy-toggle.bat on 79   使用 192.168.0.79:6478
REM   proxy-toggle.bat off     清空代理
REM   proxy-toggle.bat status  查看当前用户环境变量中的代理
REM 注意：setx 写入的是用户环境变量，新开的 GoLand/PowerShell/服务进程才会读取到。

set ACTION=%1
set NODE=%2

if "%ACTION%"=="" goto help
if /I "%ACTION%"=="status" goto status
if /I "%ACTION%"=="off" goto off
if /I "%ACTION%"=="on" goto on
goto help

:on
set PROXY_HOST=
if "%NODE%"=="73" set PROXY_HOST=192.168.0.73:6478
if "%NODE%"=="74" set PROXY_HOST=192.168.0.74:6478
if "%NODE%"=="79" set PROXY_HOST=192.168.0.79:6478
if "%PROXY_HOST%"=="" goto help

set PROXY_URL=http://%PROXY_HOST%
setx HTTP_PROXY "%PROXY_URL%" >nul
setx HTTPS_PROXY "%PROXY_URL%" >nul
setx ALL_PROXY "%PROXY_URL%" >nul
echo 已设置代理：%PROXY_URL%
echo 请重新打开 GoLand 或 PowerShell 后再启动 Go 服务。
goto end

:off
setx HTTP_PROXY "" >nul
setx HTTPS_PROXY "" >nul
setx ALL_PROXY "" >nul
echo 已清空 HTTP_PROXY / HTTPS_PROXY / ALL_PROXY。
echo 请重新打开 GoLand 或 PowerShell 后再启动 Go 服务。
goto end

:status
echo HTTP_PROXY=%HTTP_PROXY%
echo HTTPS_PROXY=%HTTPS_PROXY%
echo ALL_PROXY=%ALL_PROXY%
goto end

:help
echo 用法：
echo   proxy-toggle.bat on 73
echo   proxy-toggle.bat on 74
echo   proxy-toggle.bat on 79
echo   proxy-toggle.bat off
echo   proxy-toggle.bat status

:end
