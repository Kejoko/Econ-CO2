# Economic Indicators Relation to CO2 Emissions
**Deionus Bauer**

**Keegan Kochis**

**Landon Zweigle**

This is a term project for CS 455 Distributed Systems. This project examines world indicator variable data and determines the relation between economic indicator variables and CO2 emission variables.

## Project Setup
#### First time
1. Download the data from [kaggle](https://www.kaggle.com/worldbank/world-development-indicators/download)
2. Unzip the archive
    * By default the resulting directory is named archive
3. Add the unarchived directory to your school machine (if you're not already on your school machine)
    * This may take a while as there is nearly 2gb of data to transfer
4. Start your hadoop cluster
5. Run: `$HADOOP_HOME/bin/hadoop fs -put -f <unarchived_dir> /wdi_data`
    * This puts the unarchived data directory into the hadoop cluster at location `/wdi_data`
6. Run: `$HADOOP_HOME/bin/hadoop fs -ls /wdi_data`
    * It should list all of the files in the dataset. This ensures you properly put the dataset in your cluster
7. Run: `source init.sh`
    * This downloads and sets up spark
    
#### Regular setup
1. Cd into this directory
2. Run the startup script via `source startup.sh`
    * This script starts your spark cluster and exports an environment variable `SPARK_SUBMIT` making it easy to submit spark jobs

