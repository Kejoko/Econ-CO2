package wdi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HelloSpark {

	public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App");
//                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("hdfs://annapolis:47501/home/wdi_data/indicators.csv");
//        JavaRDD<String> stringJavaRDD = spark.read.csv("hdfs://annapolis:47501/home/wdi_data/indicators.csv");
        System.out.println("Number of lines in file = " + stringJavaRDD.count());
	}

}
