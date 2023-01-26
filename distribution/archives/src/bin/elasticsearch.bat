@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set CLI_NAME=serverless-server
set CLI_LIBS=lib/tools/server-cli,lib/tools/serverless-server-cli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
