#!/bin/bash

BOLD=$(tput bold)
NORMAL=$(tput sgr0)

RED=$(tput setaf 1)
YELLOW=$(tput setaf 3)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
NOCOLOR=$(tput setf 9)

## Set the proper directories, jars, and classes based on input argument
#HADOOP_BIN="${HADOOP_HOME}/bin/hadoop"
#INPUT_DIR="/home/wdi_data"
#OUTPUT_DIR="/home/co2_out"
#JAR_FILE="build/libs/Econ-CO2.jar"
#MAIN_CLASS="wdi.WDImain"
#
## Remove the output directory before attempting to write to it
#{
#    echo "${BOLD}${YELLOW}REMOVING $OUTPUT_DIR ${NORMAL}${NOCOLOR}"
#    $HADOOP_BIN fs -rm -r $OUTPUT_DIR
#    echo " - ${BOLD}${GREEN}Successfully removed $OUTPUT_DIR ${NORMAL}${NOCOLOR}"
#} || {
#    echo "  - ${RED}Failed to remove $OUTPUT_DIR.${NORMAL}${NOCOLOR}"
#}
#
## Submit the job to run the jar
#{
#    HADOOP_JAR_CMD="$HADOOP_BIN jar $JAR_FILE $MAIN_CLASS $INPUT_DIR $OUTPUT_DIR"
#    echo "${BOLD}${CYAN}RUNNING JAR${NORMAL}${NOCOLOR}"
#    echo "  -${CYAN} $HADOOP_JAR_CMD ${NORMAL}${NOCOLOR}"
#    $HADOOP_JAR_CMD
#    echo " - ${BOLD}${GREEN}Successfully executed jar ${NORMAL}${NOCOLOR}"
#} || {
#    echo "  - ${RED}Failed to execute jar.${NORMAL}${NOCOLOR}"
#}

# Run the SparkHelloWorld java class
$SPARK_SUBMIT --master spark://des-moines:50000 --deploy-mode cluster --class wdi.SparkHelloWorld build/libs/Econ-CO2.jar
