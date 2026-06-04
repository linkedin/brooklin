@echo off


SetLocal
IF ["%LOG4J_OPTS%"] EQU [""] (
    set LOG4J_OPTS=-Dlog4j.configurationFile=file:%~dp0../../config/log4j2.properties
)
IF ["%HEAP_OPTS%"] EQU [""] (
    set HEAP_OPTS=-Xmx1G -Xms1G
)

%~dp0run-class.bat com.linkedin.datastream.tools.DatastreamRestClientCli %*
EndLocal
