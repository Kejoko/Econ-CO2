package wdi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class SparkHelloWorld {
    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("Spark Hello World").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //sparkContext.addJar("file:///Econ-C02.jar");
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("file:///s/chopin/n/under/deionus/WDIDataset/Indicators.csv");

        //TEST RUN FOR MERCHANDISE EXPORTS (TX.VAL.MRCH.CD.WT)
        //CO2 EN.ATM.CO2E.PC

        //Filter out everything except the entries with the correct Indicator Code
        JavaRDD<String> filterByExports = stringJavaRDD.filter((Function<String, Boolean>) line -> {

            //split the line into tokens
            String[] tokens = line.split(",", -1);

            //If the token has a value, return true if it has the correct code
            return tokens[3] != null && tokens[3].equals("TX.VAL.MRCH.CD.WT");
        });

        JavaRDD<String> filterByCO2 = stringJavaRDD.filter((Function<String, Boolean>) line -> {

            //split the line into tokens
            String[] tokens = line.split(",", -1);

            //If the token has a value, return true if it has the correct code
            return tokens[3] != null && tokens[3].equals("EN.ATM.CO2E.PC");
        });

        //Map the datasets to KEY VALUE pair
        JavaPairRDD<String, Double> pairExports = pair(filterByExports);
        JavaPairRDD<String, Double> pairCO2 = pair(filterByCO2);

        JavaPairRDD<String, Tuple2<Double, Double>> joined = pairExports.join(pairCO2);

        List<Tuple2<String, Tuple2<Double, Double>>> collection = joined.collect();

        System.out.println("Number of Tuples = " + joined.count());
        for (Tuple2<String , Tuple2<Double, Double>> tuple : collection) {
            if (tuple._1.contains("ARB"))
                System.out.println(tuple._1 + " (" + tuple._2._1 + "," + tuple._2._2 + ")");
        }
    }

    //Method which calls MapToPair and returns an RDD with a key of Country Code and a Value
    private static JavaPairRDD<String, Double> pair(JavaRDD<String> RDD) {
        return RDD.mapToPair((PairFunction<String, String, Double>) line -> {

            //split the line into tokens
            String[] tokens = line.split(",", -1);

            //Get the country code
            String countryCode = tokens[1];
            String year = tokens[4];
            if (countryCode == null || countryCode.length() == 0)
                countryCode = "BadCode";
            if (year == null || year.length() == 0)
                year = "BadYear";

            String key = countryCode+year;

            Double value;
            //Get the value
            try {
                value = Double.parseDouble(tokens[5]);
            }
            catch (NumberFormatException e)
            {
                value = 0.0;
            }

            return new Tuple2<>(key, value);
        });
    }
}