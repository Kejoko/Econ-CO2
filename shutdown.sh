#!/bin/bash

BOLD=$(tput bold)
NORMAL=$(tput sgr0)

RED=$(tput setaf 1)
YELLOW=$(tput setaf 3)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
NOCOLOR=$(tput setf 9)

# stop yarn
{
    echo "${BOLD}${YELLOW}STOPPING YARN${NORMAL}${NOCOLOR}"
    $HADOOP_HOME/sbin/stop-yarn.sh
    echo "${BOLD}${GREEN}STOPPED YARN${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to stop yarn.${NORMAL}${NOCOLOR}"
}

# stop hdfs
{
    echo "${BOLD}${YELLOW}STOPPING HDFS${NORMAL}${NOCOLOR}"
    $HADOOP_HOME/sbin/stop-dfs.sh
    echo "${BOLD}${GREEN}STOPPED HDFS${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to stop hdfs.${NORMAL}${NOCOLOR}"
}
