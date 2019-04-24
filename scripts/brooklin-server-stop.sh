#!/bin/sh
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps ax | grep -i 'datastream' | grep java | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No Brooklin server to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi
