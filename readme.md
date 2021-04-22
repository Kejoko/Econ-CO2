# Economic Indicators Relation to CO2 Emissions
**Deionus Bauer**

**Keegan Kochis**

**Landon Zweigle**

This is a term project for CS 455 Distributed Systems. This project examines world indicator variable data and determines the relation between economic indicator variables and CO2 emission variables.

## Project Setup
#### First time
1. Download the data from [kaggle](https://www.kaggle.com/worldbank/world-development-indicators/download)
1. Unzip the archive
1. Move the unzipped archive to your home directory and rename it `WDIDataset`
1. Start your hadoop cluster
1. Run: `$HADOOP_HOME/bin/hadoop fs -put -f <unarchived_dir> /wdi_data`
    * This puts the unarchived data directory into the hadoop cluster at location `/wdi_data`
1. Run: `$HADOOP_HOME/bin/hadoop fs -ls /wdi_data`
    * It should list all of the files in the dataset. This ensures you properly put the dataset in your cluster
1. Run: `source init.sh`
    * This downloads and sets up spark
    
#### Regular setup
1. Cd into this directory
1. Run the startup script via `source startup.sh`
    * This script starts your spark cluster and exports an environment variable `SPARK_SUBMIT` making it easy to submit spark jobs
1. SSH tunnel to view the running jobs
    * `ssh -L 49999:des-moines.cs.colostate.edu:50001 <username>@des-moines.cs.colostate.edu`
    * Navigate to `localhost:49999` in web browser
2. Run `./runjar.sh` to submit a job to the cluster
