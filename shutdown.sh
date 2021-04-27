#!/bin/bash

BOLD=$(tput bold)
NORMAL=$(tput sgr0)

RED=$(tput setaf 1)
YELLOW=$(tput setaf 3)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
NOCOLOR=$(tput setf 9)

# stop spark cluster

# Start spark cluster
{
    echo ""
    echo ""
    echo "${CYAN}Attempting:${NORMAL}${NOCOLOR} Stopping spark cluster"
    $SPARK_HOME/sbin/stop-all.sh
    echo "${BOLD}${GREEN}SUCCESS:${NORMAL}${NOCOLOR} Stopped spark cluster"
} || {
    echo "  - ${RED}Failed to stop spark cluster.${NORMAL}${NOCOLOR}"
}
