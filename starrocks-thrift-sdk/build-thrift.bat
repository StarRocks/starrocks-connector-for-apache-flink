@echo off
if not exist thrift md thrift
cd /d thrift
if not exist thrift.exe (
    certutil -urlcache -split -f http://dlcdn.apache.org/thrift/0.13.0/thrift-0.13.0.exe thrift.exe
)
start thrift.exe -r -gen java %~dp0%\gensrc\StarrocksExternalService.thrift
cd %~p0%
if not exist src\main\java\com\starrocks\thrift md src\main\java\com\starrocks\thrift 
pause
copy thrift\gen-java\com\starrocks\thrift src\main\java\com\starrocks\thrift
pause