@echo off

wmic process where (commandline like "%%datastream%%" and not name="wmic.exe") delete
rem ps ax | grep -i 'datastream' | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM
