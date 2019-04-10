#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-daemon] [-name servicename] [-loggc] classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/..

# run ./gradlew copyDependentLibs to get all dependent jars in a local dir

module_dirs=("datastream-server"
             "datastream-kafka"
             "datastream-tools"
             "datastream-file-connector"
             "datastream-server-restli"
             )

for dir in "${module_dirs[@]}";
do
  if [ -d "$base_dir/$dir/build/libs" ]; then
    CLASSPATH=$CLASSPATH:"$base_dir/$dir/build/libs/*"
  fi

  if [ -d "$base_dir/$dir/build/dependent-libs" ]; then
    CLASSPATH=$CLASSPATH:"$base_dir/$dir/build/dependent-libs/*"
  fi
done

# classpath addition for release
if [ -d "$base_dir/libs" ]; then
  CLASSPATH=$CLASSPATH:"$base_dir/libs/*"
fi


# JMX settings
if [ -z "$JMX_OPTS" ]; then
  JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi

# JMX port to use
if [  $JMX_PORT ]; then
  JMX_OPTS="$JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

# Log directory to use
if [ "x$LOG_DIR" = "x" ]; then
    LOG_DIR="$base_dir/logs"
fi

# Log4j settings
if [ -z "$LOG4J_OPTS" ]; then
  # Log to console. This is a tool.
  LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/config/tools-log4j.properties"
else
  # create logs directory
  if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
  fi
fi

LOG4J_OPTS="-Ddatastream.logs.dir=$LOG_DIR $LOG4J_OPTS"

# Generic jvm settings you want to add
if [ -z "$OPTS" ]; then
  OPTS=""
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Memory options
if [ -z "$HEAP_OPTS" ]; then
  HEAP_OPTS="-Xmx256M"
fi

# JVM performance options
if [ -z "$JVM_PERFORMANCE_OPTS" ]; then
  JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi


while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -loggc)
      if [ -z "$GC_LOG_OPTS" ]; then
        GC_LOG_ENABLED="true"
      fi
      shift
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

# GC options
GC_FILE_SUFFIX='-gc.log'
GC_LOG_FILE_NAME=''
if [ "x$GC_LOG_ENABLED" = "xtrue" ]; then
  GC_LOG_FILE_NAME=$DAEMON_NAME$GC_FILE_SUFFIX
  GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps "
fi

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  nohup $JAVA $HEAP_OPTS $JVM_PERFORMANCE_OPTS $GC_LOG_OPTS $JMX_OPTS $LOG4J_OPTS -cp $CLASSPATH $OPTS "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
else
  exec $JAVA $HEAP_OPTS $JVM_PERFORMANCE_OPTS $GC_LOG_OPTS $JMX_OPTS $LOG4J_OPTS -cp $CLASSPATH $OPTS "$@"
fi
