package wdi;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public class SparkHelloWorld {
    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("Spark Hello World").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //sparkContext.addJar("file:///Econ-C02.jar");
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("file:///s/chopin/n/under/deionus/WDIDataset/Indicators.csv");
        System.out.println("Number of lines in file = " + stringJavaRDD.count());
    }
}