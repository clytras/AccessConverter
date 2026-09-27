@echo off
rem AccessConverter: runs the Java runtime bundled next to this script, never an installed one.
rem ACCESSCONVERTER_JAVA_OPTS adds JVM options, e.g. set ACCESSCONVERTER_JAVA_OPTS=-Xmx4g
rem sqlite-jdbc loads its native library from lib\native instead of unpacking it into java.io.tmpdir, and the JVM
rem keeps no performance data there. ACCESSCONVERTER_JAVA_OPTS comes after these options, so it can override them.
setlocal
set "HOME_DIR=%~dp0.."
"%HOME_DIR%\runtime\bin\java.exe" -XX:-UsePerfData "-Dorg.sqlite.lib.path=%HOME_DIR%\lib\native" ^
    -Dorg.sqlite.lib.name=sqlitejdbc.dll %ACCESSCONVERTER_JAVA_OPTS% -jar "%HOME_DIR%\lib\accessconverter.jar" %*
exit /b %ERRORLEVEL%
