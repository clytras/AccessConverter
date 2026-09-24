@echo off
rem AccessConverter: runs the Java runtime bundled next to this script, never an installed one.
rem ACCESSCONVERTER_JAVA_OPTS adds JVM options, e.g. set ACCESSCONVERTER_JAVA_OPTS=-Xmx4g
setlocal
set "HOME_DIR=%~dp0.."
"%HOME_DIR%\runtime\bin\java.exe" %ACCESSCONVERTER_JAVA_OPTS% -jar "%HOME_DIR%\lib\accessconverter.jar" %*
exit /b %ERRORLEVEL%
