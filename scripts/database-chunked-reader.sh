#!/bin/bash

base_dir=$(dirname $0)

if [ "x$LOG4J_OPTS" = "x" ]; then
    export LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/../config/log4j.properties"
fi

if [ "x$HEAP_OPTS" = "x" ]; then
    export HEAP_OPTS="-Xmx1G -Xms1G"
fi

exec $base_dir/run-class.sh com.linkedin.datastream.tools.DatabaseChunkedReaderClient $@