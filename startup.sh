#!/bin/bash

BOLD=$(tput bold)
NORMAL=$(tput sgr0)

RED=$(tput setaf 1)
YELLOW=$(tput setaf 3)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
NOCOLOR=$(tput setf 9)


# stop yarn before starting again
{
    echo "${BOLD}${YELLOW}STOPPING YARN${NORMAL}${NOCOLOR}"
    $HADOOP_HOME/sbin/stop-yarn.sh
    echo "${BOLD}${GREEN}STOPPED YARN${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to stop yarn.${NORMAL}${NOCOLOR}"
}

# stop hdfs before starting again
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

# Start YARN
{
    echo "${BOLD}${CYAN}STARTING YARN${NORMAL}${NOCOLOR}"
    $HADOOP_HOME/sbin/start-yarn.sh
    echo "${BOLD}${GREEN}STARTED YARN${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to start yarn.${NORMAL}${NOCOLOR}"
}

# export the client config directory
export HADOOP_CONF_DIR=~/hadoopClientConf

# Check to see if the files exist in the hadoop cluster
{
    echo "${BOLD}${CYAN}Checking existence of data${NORMAL}${NOCOLOR}"
    $HADOOP_HOME/bin/hadoop fs -ls /home/wdi_data
    echo "${BOLD}${GREEN}Checked existence${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to check data existence.${NORMAL}${NOCOLOR}"
}

# export easy command for spark-submit
export SPARK_SUBMIT=/usr/local/spark/3.0.1-with-hadoop3.2/bin/spark-submit
