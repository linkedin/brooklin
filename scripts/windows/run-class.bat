@echo off


setlocal enabledelayedexpansion

IF [%1] EQU [] (
	echo USAGE: %0 classname [opts]
	EXIT /B 1
)

rem Using pushd popd to set BASE_DIR to the absolute path
pushd %~dp0..\..
set BASE_DIR=%CD%
popd

call :concat %BASE_DIR%\datastream-server\build\libs\*
call :concat %BASE_DIR%\datastream-server\build\dependent-libs\*
call :concat %BASE_DIR%\datastream-kafka\build\libs\*
call :concat %BASE_DIR%\datastream-kafka\build\dependent-libs\*
call :concat %BASE_DIR%\datastream-file-connector\build\libs\*
call :concat %BASE_DIR%\datastream-file-connector\build\dependent-libs\*
call :concat %BASE_DIR%\datastream-tools\build\libs\*
call :concat %BASE_DIR%\datastream-tools\build\dependent-libs\*


rem Classpath addition for release
call :concat %BASE_DIR%\libs\*

rem JMX settings
IF ["%JMX_OPTS%"] EQU [""] (
	set JMX_OPTS=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false
)

rem JMX port to use
IF ["%JMX_PORT%"] NEQ [""] (
	set JMX_OPTS=%JMX_OPTS% -Dcom.sun.management.jmxremote.port=%JMX_PORT%
)

rem Log directory to use
IF ["%LOG_DIR%"] EQU [""] (
    set LOG_DIR=%BASE_DIR%/logs
)

rem Log4j settings
IF ["%LOG4J_OPTS%"] EQU [""] (
	set LOG4J_OPTS=-Dlog4j.configuration=file:%BASE_DIR%/config/tools-log4j.properties
) ELSE (
  rem create logs directory
  IF not exist %LOG_DIR% (
      mkdir %LOG_DIR%
  )
)

set LOG4J_OPTS=-Ddatastream.logs.dir=%LOG_DIR% %LOG4J_OPTS%

rem Generic jvm settings you want to add
IF ["%OPTS%"] EQU [""] (
	set OPTS=
)

set DEFAULT_JAVA_DEBUG_PORT=5005
set DEFAULT_DEBUG_SUSPEND_FLAG=n
rem Set Debug options if enabled
IF ["%DEBUG%"] NEQ [""] (


	IF ["%JAVA_DEBUG_PORT%"] EQU [""] (
		set JAVA_DEBUG_PORT=%DEFAULT_JAVA_DEBUG_PORT%
	)

	IF ["%DEBUG_SUSPEND_FLAG%"] EQU [""] (
		set DEBUG_SUSPEND_FLAG=%DEFAULT_DEBUG_SUSPEND_FLAG%
	)
	set DEFAULT_JAVA_DEBUG_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=!DEBUG_SUSPEND_FLAG!,address=!JAVA_DEBUG_PORT!

	IF ["%JAVA_DEBUG_OPTS%"] EQU [""] (
		set JAVA_DEBUG_OPTS=!DEFAULT_JAVA_DEBUG_OPTS!
	)

	echo Enabling Java debug options: !JAVA_DEBUG_OPTS!
	set OPTS=!JAVA_DEBUG_OPTS! !OPTS!
)

rem Which java to use
IF ["%JAVA_HOME%"] EQU [""] (
	set JAVA=java
) ELSE (
	set JAVA="%JAVA_HOME%/bin/java"
)

rem Memory options
IF ["%HEAP_OPTS%"] EQU [""] (
	set HEAP_OPTS=-Xmx1G -Xms1G
)

rem JVM performance options
IF ["%JVM_PERFORMANCE_OPTS%"] EQU [""] (
	set JVM_PERFORMANCE_OPTS=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true
)

IF ["%CLASSPATH%"] EQU [""] (
	echo Classpath is empty. Please build the project first e.g. by running 'gradlew jarAll'
	EXIT /B 2
)

set COMMAND=%JAVA% %HEAP_OPTS% %JVM_PERFORMANCE_OPTS% %JMX_OPTS% %LOG4J_OPTS% -cp %CLASSPATH% %OPTS% %*
rem echo.
rem echo "Executing:%COMMAND%"
rem echo.

%COMMAND%

goto :eof
:concat
IF ["%CLASSPATH%"] EQU [""] (
  set CLASSPATH=%1
) ELSE (
  set CLASSPATH=%CLASSPATH%;%1
)
