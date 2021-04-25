#!/bin/bash

BOLD=$(tput bold)
NORMAL=$(tput sgr0)

RED=$(tput setaf 1)
YELLOW=$(tput setaf 3)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
NOCOLOR=$(tput setf 9)

# Submit the spark job and give the home directory for the user

# Local
/usr/local/spark/3.0.1-with-hadoop3.2/bin/spark-submit --master local --class wdi.SparkHelloWorld build/libs/Econ-CO2.jar ~

# Cluster
#$SPARK_SUBMIT --master spark://des-moines:50000 --deploy-mode cluster --class wdi.SparkHelloWorld build/libs/Econ-CO2.jar ~

