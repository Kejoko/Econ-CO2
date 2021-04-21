#!/bin/bash

BOLD=$(tput bold)
NORMAL=$(tput sgr0)

RED=$(tput setaf 1)
YELLOW=$(tput setaf 3)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
NOCOLOR=$(tput setf 9)

CURR_DIR=$(pwd)
SPARK_DIR=spark-3.1.1-bin-without-hadoop
SPARK_CONF_DIR=${SPARK_DIR}/conf
SPARK_DL_URL=https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-without-hadoop.tgz

# Download the latest release of spark
{
    echo "${BOLD}${CYAN}Downloading spark${NORMAL}${NOCOLOR}"
    wget ${SPARK_DL_URL}
    echo "${BOLD}${GREEN}Successfully downloaded spark${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to download spark.${NORMAL}${NOCOLOR}"
}

# Untar the downlaoded spark distro
{
    echo "${BOLD}${CYAN}Unpacking spark${NORMAL}${NOCOLOR}"
    tar -xzvf ${SPARK_DIR}.tgz
    echo "${BOLD}${GREEN}Successfully unpacked spark${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to unpack spark.${NORMAL}${NOCOLOR}"
}

# Rename and update slaves file
SLAVES_FILE=${SPARK_CONF_DIR}/slaves
{
    echo "${BOLD}${CYAN}Updating slaves file${NORMAL}${NOCOLOR}: ${SLAVES_FILE}"
    mv ${SLAVES_FILE}.template ${SLAVES_FILE}
    echo "atlanta" > ${SLAVES_FILE}
    echo "augusta" >> ${SLAVES_FILE}
    echo "austin" >> ${SLAVES_FILE}
    echo "baton-rouge" >> ${SLAVES_FILE}
    echo "bismarck" >> ${SLAVES_FILE}
    echo "boise" >> ${SLAVES_FILE}
    echo "boston" >> ${SLAVES_FILE}
    echo "carson-city" >> ${SLAVES_FILE}
    echo "charleston" >> ${SLAVES_FILE}
    echo "cheyenne" >> ${SLAVES_FILE}
    echo "columbia" >> ${SLAVES_FILE}
    echo "columbus-oh" >> ${SLAVES_FILE}
    echo "concord" >> ${SLAVES_FILE}
    echo "${BOLD}${GREEN}Successfully updated slaves file${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to update slaves file.${NORMAL}${NOCOLOR}"
}

# Rename and update spark-env.sh
ENV_FILE=${SPARK_CONF_DIR}/spark-env.sh
{
    echo "${BOLD}${CYAN}Updating spark-env.sh file${NORMAL}${NOCOLOR}: ${ENV_FILE}"
    mv ${ENV_FILE}.template ${ENV_FILE}
    echo "export SPARK_MASTER_IP=des-moines" >> ${ENV_FILE}
    echo "export SPARK_MASTER_PORT=50000" >> ${ENV_FILE}
    echo "export SPARK_MASTER_WEBUI_PORT=50001" >> ${ENV_FILE}
    echo "export SPARK_WORKER_CORES=2" >> ${ENV_FILE}
    echo "export SPARK_WORKER_MEMORY=2g" >> ${ENV_FILE}
    echo "export SPARK_WORKER_INSTANCES=2" >> ${ENV_FILE}
    echo "${BOLD}${GREEN}Successfully updated spark-env.sh file${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to update spark-env.sh file.${NORMAL}${NOCOLOR}"
}

# Rename and update spark-defaults.conf
DEFAULTS_FILE=${SPARK_CONF_DIR}/spark-defaults.conf
{
    echo "${BOLD}${CYAN}Updating spark-defaults.conf file${NORMAL}${NOCOLOR}: ${DEFAULTS_FILE}"
    mv ${DEFAULTS_FILE}.template ${DEFAULTS_FILE}
    echo "spark.master          spark://des-moines:50000" >> ${DEFAULTS_FILE}
    echo "${BOLD}${GREEN}Successfully updated spark-defaults.conf file${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to update spark-defaults.conf file.${NORMAL}${NOCOLOR}"
}

# Update the bashrc with the name of new spark dir
{
    echo "${BOLD}${CYAN}Updating ~/.bashrc${NORMAL}${NOCOLOR}"
#    echo "" >> ~/.bashrc
#    echo "" >> ~/.bashrc
#    echo "# Spark home directory for cs455 term project" >> ~/.bashrc
#    echo "export SPARK_HOME=${CURR_DIR}/${SPARK_DIR}" >> ~/.bashrc
    echo ""
    echo ""
    echo "# Spark home directory for cs455 term project"
    echo "export SPARK_HOME=${CURR_DIR}/${SPARK_DIR}"
    source ~/.bashrc
    echo "${BOLD}${GREEN}Successfully updated ~/.bashrc${NORMAL}${NOCOLOR}"
} || {
    echo "  - ${RED}Failed to update ~/.bashrc.${NORMAL}${NOCOLOR}"
}
