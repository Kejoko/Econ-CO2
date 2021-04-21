#!/bin/bash

BOLD=$(tput bold)
NORMAL=$(tput sgr0)

RED=$(tput setaf 1)
YELLOW=$(tput setaf 3)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
NOCOLOR=$(tput setf 9)

# Start spark cluster
{
    echo ""
    echo ""
    echo "${CYAN}Attempting:${NORMAL}${NOCOLOR} Starting spark cluster"
    $SPARK_HOME/sbin/start-all.sh
    echo "${BOLD}${GREEN}SUCCESS:${NORMAL}${NOCOLOR} Started spark cluster"
} || {
    echo "  - ${RED}Failed to start spark cluster.${NORMAL}${NOCOLOR}"
}

# export easy command for spark-submit
#export SPARK_SUBMIT=/usr/local/spark/3.0.1-without-hadoop/bin/spark-submit
export SPARK_SUBMIT=${SPARK_HOME}/bin/spark-submit
