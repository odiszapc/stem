@REM Copyright 2014 Alexey Plotnik
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.

@echo off
if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED STEM_MANAGER_HOME set STEM_MANAGER_HOME=%CD%
popd

if NOT DEFINED STEM_MANAGER_MAIN set STEM_MANAGER_MAIN=org.stem.ClusterManagerDaemon
if NOT DEFINED JAVA_HOME goto :err

set CLASSPATH="%STEM_MANAGER_HOME%\conf"

set JAVA_OPTS=-ea^
  -Xms256M^
  -Xmx512M^
  -XX:+HeapDumpOnOutOfMemoryError^
  -XX:+UseParNewGC^
  -XX:+UseConcMarkSweepGC^
  -XX:+CMSParallelRemarkEnabled^
  -XX:SurvivorRatio=8^
  -XX:MaxTenuringThreshold=1^
  -XX:CMSInitiatingOccupancyFraction=75^
  -XX:+UseCMSInitiatingOccupancyOnly^
  -Dlogback.configurationFile=logback.xml

for %%i in ("%STEM_MANAGER_HOME%\lib\*.jar") do call :append "%%i"
goto runDaemon

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

:runDaemon
"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp %CLASSPATH% "%STEM_MANAGER_MAIN%"
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally

ENDLOCAL

