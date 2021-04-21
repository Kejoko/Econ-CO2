package wdi;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HelloSpark {

	public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("hdfs://annapolis:47501/home/wdi_data/indicators.csv");
//        System.out.println("Number of lines in file = " + stringJavaRDD.count());
        
        List<String> listCsvLines = stringJavaRDD.collect();
        System.out.println("========= " + listCsvLines);
        sparkContext.close();
	}

}
