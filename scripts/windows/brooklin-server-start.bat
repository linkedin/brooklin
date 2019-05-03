@echo off

IF [%1] EQU [] (
	echo USAGE: %0 server.properties
	EXIT /B 1
)

SetLocal
IF ["%LOG4J_OPTS%"] EQU [""] (
    set LOG4J_OPTS=-Dlog4j.configuration=file:%~dp0../../config/log4j.properties
)
IF ["%HEAP_OPTS%"] EQU [""] (
    set HEAP_OPTS=-Xmx1G -Xms1G
)
%~dp0run-class.bat com.linkedin.datastream.server.DatastreamServer %*
EndLocal
