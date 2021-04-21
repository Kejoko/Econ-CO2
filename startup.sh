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
    echo "${BOLD}${CYAN}Starting spark cluster${NORMAL}${NOCOLOR}"
    $SPARK_HOME/sbin/start-all.sh
    echo "${BOLD}${GREEN}Started spark cluster${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to start spark Master.${NORMAL}${NOCOLOR}"
}

# export easy command for spark-submit
#export SPARK_SUBMIT=/usr/local/spark/3.0.1-without-hadoop/bin/spark-submit
export SPARK_SUBMIT=${SPARK_HOME}/bin/spark-submit
