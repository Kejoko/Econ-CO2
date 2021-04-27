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
1. Run: `source init.sh`
    * This downloads and sets up spark
1. Run `gradle build`
    
#### Run the spark job on the cluster
**Make sure you're logged into `des-moines`**
    * This is the master for the cluster created with the `init.sh ` script
1. Cd into this directory
1. Run the startup script via `source startup.sh`
    * This script starts your spark cluster and exports an environment variable `SPARK_SUBMIT` making it easy to submit spark jobs
1. Navigate to `localhost:50001` in a web browser to see the running jobs
1. Run `./runjar.sh cluster` to submit a job to the cluster

#### Run a Spark Job locally
1. Cd into this directory
1. `./runjar.sh`
