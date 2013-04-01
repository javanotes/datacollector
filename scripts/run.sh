#!/bin/sh 
# COPYRIGHT (C) ERICSSON INTERNET APPLICATIONS INC.
#
# THIS SOFTWARE IS FURNISHED UNDER A LICENSE ONLY AND IS
# PROPRIETARY TO ERICSSON INTERNET APPLICATIONS INC. IT MAY NOT BE COPIED
# EXCEPT WITH THE PRIOR WRITTEN PERMISSION OF ERICSSON INTERNET APPLICATIONS
# INC.  ANY COPY MUST INCLUDE THE ABOVE COPYRIGHT NOTICE AS
# WELL AS THIS PARAGRAPH.  THIS SOFTWARE OR ANY OTHER COPIES
# THEREOF, MAY NOT BE PROVIDED OR OTHERWISE MADE AVAILABLE
# TO ANY OTHER PERSON OR ENTITY.
# TITLE TO AND OWNERSHIP OF THIS SOFTWARE SHALL AT ALL
# TIMES REMAIN WITH ERICSSON INTERNET APPLICATIONS INC.
#

############### Setup various constants ################
INSTALLDIR=/home/esutdal/workspace/github/data-collector
JARDIR=${INSTALLDIR}/lib
APP_CONFIG=${INSTALLDIR}/config/datacollector.properties
COMPONENT_NAME="FmtDataCollector"
PROCESS_TO_MONITOR="fmt.DataCollector"
LOG_FILE=/home/esutdal/log/data-collector/run.log
JAVA_HOME=/home/esutdal/Tools/jdk1.7.0_17/jre

PATH=$JAVA_HOME/bin:$PATH
########################################################

# Setup java class path
# Add lib directory to class path
libs=`ls $JARDIR/*.jar`
for lib in $libs
do
   CLASSPATH=$CLASSPATH":"$lib
done

LD_LIBRARY_PATH=$JARDIR

# Allow remote debug. Commented-out by default.
#JVM_FLAGS="$JVM_FLAGS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4000"

JVM_FLAGS="$JVM_FLAGS -DCompType=${COMPTYPE}"
JVM_FLAGS="$JVM_FLAGS -Xms128m -Xmx128m"
JVM_FLAGS="$JVM_FLAGS -XX:+UseParallelOldGC"

START_CMD="$JAVA_HOME/bin/java -server -D$PROCESS_TO_MONITOR $JVM_FLAGS -cp $CLASSPATH com.egi.datacollector.startup.Main -c $APP_CONFIG"

#############################################################################
# Prints the process id of the process ${PROCESS_TO_MONITOR}
#############################################################################
function getPid() {
    local mypid=`ps auxwwww | grep "$PROCESS_TO_MONITOR" | grep -v grep | awk '{ printf( "%s ", $2 );}'`
    echo "$mypid"
}

#############################################################################
# Logs a message to stdout
#############################################################################
function log() {
    echo "$@"
    
}


#############################################################################
# Show component status.
#
# Return values:
#   0: Component is running
#   1: Component is not running
#############################################################################
function showComponentStatus() {
    nbprocesses=`ps -ef | grep "$PROCESS_TO_MONITOR" | grep -v grep | wc -l`
    if [ ${nbprocesses} -gt 0 ]; then
	log "${COMPONENT_NAME} has ${nbprocesses} instance/s running with process id: $(getPid)"
	return 0
    else
        log "${COMPONENT_NAME} is not running"
        return 1
    fi
}
 
#############################################################################
# Graceful shutdown of the component
#
# Return values:
#   0: Component shutdown signal was sent to the process
#   1: Component is not running
#############################################################################
function stopComponent() {
   
    if [ "$1" = "cluster" ]; then 
	pid=$(getPid)
	log "Shutting down ${COMPONENT_NAME} cluster"
	
	for i in $pid
	do	
		log "Stopping ${COMPONENT_NAME} instance (pid:$i)"
		kill -15 $i
	done
        
        return 0
    elif [ ! -z "$1" ]; then
	pid=$1
        log "Stopping ${COMPONENT_NAME} instance (pid:$pid)"
	kill -15 $pid
        return 0
    else
	log "${COMPONENT_NAME} is not running"
        return 1
    fi
}

#############################################################################
# Forceful kill of the component
#
# Return values:
#   0: Kill signal was sent to the process
#   1: Component is not running
#############################################################################
function killComponent() {
        
    if [ ! -z "$1" ]; then 
	pid=$1
        log "Killing ${COMPONENT_NAME} process (pid:$pid)"
        kill -9 $pid
        return 0
    else
        log "Process id missing"
        return 1
    fi
}

#############################################################################
# Start the main process of the component
#
# Return values:
#   0: Component process was started
#   1: Component failed to start
#   2: Component is already running
#   3: Wrong user.  Refusing to start component.
#############################################################################
function startComponent() {   
                       
        echo "Starting ${COMPONENT_NAME} instance. Please wait..."        
        
        # Using bash to prevent nulls when rotating log files - see TR HH68130
        bash -c "(${START_CMD} 2>&1) >> $LOG_FILE &"

        i=1 ; max=7
        while [ $i -le $max ]
        do
            echo -n .
            sleep 1
            i=`expr $i + 1`;
        done
        echo .
        showComponentStatus
        return $?
    
}

function showPrimary() {

	nbprocesses=`ps -ef | grep "$PROCESS_TO_MONITOR" | grep -v grep | wc -l`
	if [ $nbprocesses -eq 0 ]; then
		log "No ${COMPONENT_NAME} instances are running"
		return 0
	else
		config_file=$(grep 'datacollector.instance.primary' $APP_CONFIG  | cut -f2 -d'=')
		pid=`head -n 1 $config_file`
		log "Primary instance is process id: $pid"
		return 0
	fi

}

case $1 in 
'status')
        showComponentStatus
    ;;
'start')
        startComponent
    ;;
'stop')
        stopComponent $2
    ;;

*)
    echo "usage: $0 {start | stop {pid}/cluster | status }"
    exit 1
    ;;

esac
exit $?
