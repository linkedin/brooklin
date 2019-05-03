#!/bin/bash

if [ $# -lt 1 ];
then
	echo "USAGE: $0 [-daemon] server.properties [--override property=value]*"
	exit 1
fi
base_dir=$(dirname $0)

if [ "x$LOG4J_OPTS" = "x" ]; then
    export LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/../config/log4j.properties"
fi

if [ "x$HEAP_OPTS" = "x" ]; then
    export HEAP_OPTS="-Xmx1G -Xms1G"
fi

EXTRA_ARGS="-name brooklin -loggc"

COMMAND=$1
case $COMMAND in
  -daemon)
    EXTRA_ARGS="-daemon "$EXTRA_ARGS
    shift
    ;;
  *)
    ;;
esac

exec $base_dir/run-class.sh $EXTRA_ARGS com.linkedin.datastream.server.DatastreamServer $@
