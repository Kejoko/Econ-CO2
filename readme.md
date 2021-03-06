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
1. Run `./shutdown.sh` to shutdown the cluster once the job has finished and you have viewed the results

#### Run a Spark Job locally
1. Cd into this directory
1. `./runjar.sh`

#### Options
* `./runjar.sh <mode> graph` creates a csv file containing the co2 emission value and gdp per capita value for each country code + year combo
    * This option is included for us to create a visual graph plotting co2 against gdp per capita
* `./runjar.sh <mode> true` normalizes the data using min max normalization after culling the outliers
    * This option is included to see if there was a difference between normalized data and not normalized data. There was no difference

## Explanation
* All of the processing is done in `src/main/java/wdi/CorelationCalculator.java`
* First an rdd is created from the dataset
* A pair rdd is created by filtering the lines containing the CO2 emissions indicator
    * The pairs are in the format (<country code><year>, <value>)
    * The outliers are removed using IQR score
    * The max, min, and mean of the set are calculated
* For each of the following economic indicator variables the following operations are performed
    * Create pair rdd by filtering the file rdd for the lines containing the desired indicator
        * The pairs are in the format (<country code><year>, <value>)
        * The outliers are removed using IQR score
        * The max, min, and mean of the set are calculated
    * Create a pair rdd by mapping each <country code><year> key to a tuple containing the emissions value and economic value
    * The correlation coefficient is calculated using Pearson's Correlation Coefficient method
