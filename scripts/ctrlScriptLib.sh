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
# FILE: ctrlScriptLib
#
# MsgCore Control Script Library
# 
# Required parameters
# -------------------
# COMPONENT_NAME     : the name to be printed out in info/error messages
# PROCESS_TO_MONITOR : the actual Java class (inc. the package) to be started/monitored
# LOG_FILE           : the log file for this component
# LOGROTATE_CONFIG   : the full path and name of the logrotate configuration file
# LOGROTATE_STATUS   : the full path and name of the logrotate status file
# START_CMD          : the command used to start the component
# RUN_AS             : the user that should be starting the process

SCRIPTNAME=`basename "$0"`

#############################################################################
# Prints the process id of the process ${PROCESS_TO_MONITOR}
#############################################################################
function getPid() {
    local mypid=`ps auxwwww | grep "$PROCESS_TO_MONITOR" | grep -v grep | awk '{print $2}' | tail -1`
    echo "$mypid"
}

#############################################################################
# Logs a message to the linux system log and to stdout
#############################################################################
function log() {
    echo "$@"
    /bin/logger -t "$SCRIPTNAME" "$@"
}

#############################################################################
# Verify the user running this script is ${RUN_AS}.
# Return 1 if not, 0 if he is.
#############################################################################
function verifyUser() {
    CURRENTUSER=`whoami`
    
    if [ ${CURRENTUSER} != ${RUN_AS} ];then
        log Current user is $CURRENTUSER, not "${RUN_AS}".  Refusing to start ${COMPONENT_NAME}.
        log Please re-run this script as user "${RUN_AS}".
        return 1
    else
        return 0
    fi
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
    
    if [ ${nbprocesses} -gt 1 ]; then
         log "[WARNING] ${COMPONENT_NAME} has ${nbprocesses} processes running.  Please stop/kill them all and restart ${COMPONENT_NAME}."
    fi
    pid=$(getPid)
        
    if [ $pid ]; then
        log "${COMPONENT_NAME} is running (pid:$pid)"
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
    # Return immediately if user is not the expected one
    #verifyUser || return 3;

    pid=$(getPid)

    if [ $pid ]; then 
        log "Stopping ${COMPONENT_NAME} process (pid:$pid)"
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
    # Return immediately if user is not the expected one
    #verifyUser || return 3;

    pid=$(getPid)

    if [ $pid ]; then 
        log "Killing ${COMPONENT_NAME} process (pid:$pid)"
        kill -9 $pid
        return 0
    else
        log "${COMPONENT_NAME} is not running"
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
    pid=$(getPid)
    
    if [ $pid ]; then
        log "${COMPONENT_NAME} is already running (pid:$pid)"
        return 2
    else
        # Return immediately if user is not the expected one
        #verifyUser || return 3;
        
                
        echo "Starting ${COMPONENT_NAME}. Please wait..."        
        
        # Using bash to prevent nulls when rotating log files - see TR HH68130
        bash -c "(${START_CMD} 2>&1) >> $LOG_FILE &"

        i=1 ; max=5
        while [ $i -le $max ]
        do
            echo -n .
            sleep 1
            i=`expr $i + 1`;
        done
        echo .
        showComponentStatus
        return $?
    fi
}

#############################################################################
# Rotate the log file of the component, according to the component's log
# rotation policy.  If the logrotate config file cannot be found, does a
# simple backup of the log file.
#
# Parameter 1: if specified, forces log rotation regardless of configuration
# setting.
#
# Return values:
#   0: logrotate was done successfully
#   2: logrotate returned an error
#   3: Invalid user
#############################################################################
function rotateLog() {
    # Return immediately if user is not the expected one
    verifyUser || return 3;
    
    if [ ! -f $LOG_FILE ]
    then
        # Log file doesn't exist - make sure its log directory exists
        # else we won't be able to create the log file
        local LOG_DIR=`dirname $LOG_FILE`;

        if [ ! -d $LOG_DIR ]
        then
            mkdir -p $LOG_DIR
        fi
    fi

    if [ ! -f ${LOGROTATE_CONFIG} ]
    then
        # When there's no logrotate config, we rotate only if it's forced.  This is not ideal, but it will prevent frequent
        # backups should the config file be missing for components that have a cron job that calls this very often.
        # Right now, we call this function with the force option only at component startup.
        # It's not ideal because in such a case, calling rotateLog from the command line will have no effect.
        if [ ! -z "$1" ]
        then
            log "Could not read logrotate config file '${LOGROTATE_CONFIG}' for ${COMPONENT_NAME}. Performing simple log backup."
            mv -v $LOG_FILE $LOG_FILE.backup
        fi
    else
        if [ ! -z "$1" ]
        then
            log "Forcing rotation of logs of ${COMPONENT_NAME}"    
            local force="--force"
        else
            log "Rotating logs of ${COMPONENT_NAME} using rules in ${LOGROTATE_CONFIG}"    
            local force=""
        fi
    
        /usr/sbin/logrotate ${force} -s ${LOGROTATE_STATUS} ${LOGROTATE_CONFIG}
        local result=$?
        
        if [ ${result} -ne 0 ]
        then
            log "Failed to rotate logs of ${COMPONENT_NAME}.  logrotate exit code is ${result}."
            return 2
        fi
    fi

    return 0
}
