#!/bin/bash

BOLD=$(tput bold)
NORMAL=$(tput sgr0)

RED=$(tput setaf 1)
YELLOW=$(tput setaf 3)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
NOCOLOR=$(tput setf 9)

# Get the command line arg (local or cluster)
LOCAL_STR="local"
CLUSTER_STR="cluster"
if [ "$#" -lt 1 ]
then
    echo "No command line argument given. Submitting job locally."
    MODE=$LOCAL_STR
elif [ "$#" -eq 1 ]
then
    MODE=$1
else
    echo "Too many arguments given. Please pass only either $LOCAL_STR or $CLUSTER_STR."
    exit 1
fi

if [ "$MODE" = "$LOCAL_STR" ] || [ "$MODE" = "$CLUSTER_STR" ]
then
    echo "Running on $MODE"
else
    echo "$MODE is not a valid option. Please pass only either $LOCAL_STR or $CLUSTER_STR."
    exit 1
fi

# Submit the spark job either locally or to the cluster and give the home directory for the user

# Local
if [ "$MODE" = "$LOCAL_STR" ]
then
    echo "Running command: ${GREEN}/usr/local/spark/3.0.1-with-hadoop3.2/bin/spark-submit --master local --class wdi.CorrelationCalculator build/libs/Econ-CO2.jar ~ local${NORMAL}"
    /usr/local/spark/3.0.1-with-hadoop3.2/bin/spark-submit --master local --class wdi.CorrelationCalculator build/libs/Econ-CO2.jar ~ local
fi

# Cluster
if [ "$MODE" = "$CLUSTER_STR" ]
then
    echo "Running command: ${GREEN}$SPARK_SUBMIT --master spark://des-moines:50000 --deploy-mode cluster --class wdi.CorrelationCalculator build/libs/Econ-CO2.jar ~ cluster${NORMAL}"
    $SPARK_SUBMIT --master spark://des-moines:50000 --deploy-mode cluster --class wdi.CorrelationCalculator build/libs/Econ-CO2.jar ~ cluster
fi
