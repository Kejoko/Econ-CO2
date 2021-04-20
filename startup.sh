#!/bin/bash

BOLD=$(tput bold)
NORMAL=$(tput sgr0)

RED=$(tput setaf 1)
YELLOW=$(tput setaf 3)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
NOCOLOR=$(tput setf 9)

{
    echo "${BOLD}${YELLOW}STOPPING HDFS${NORMAL}${NOCOLOR}"
    $HADOOP_HOME/sbin/stop-dfs.sh
    echo "${BOLD}${GREEN}STOPPED HDFS${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to stop hdfs.${NORMAL}${NOCOLOR}"
}

# Start HDFS
{
    echo "${BOLD}${CYAN}STARTING HDFS${NORMAL}${NOCOLOR}"
    $HADOOP_HOME/sbin/start-dfs.sh
    echo "${BOLD}${GREEN}STARTED HDFS${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to start hdfs.${NORMAL}${NOCOLOR}"
}

# Check to see if the files exist in the hadoop cluster
