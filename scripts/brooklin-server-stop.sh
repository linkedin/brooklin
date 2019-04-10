#!/bin/sh

ps ax | grep -i 'datastream' | grep java | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM
