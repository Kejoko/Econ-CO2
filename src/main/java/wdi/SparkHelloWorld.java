package wdi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SparkHelloWorld {
    public static void main(String[] args) throws IOException {
    	SparkConf sparkConf = new SparkConf().setAppName("Spark Hello World").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("file://" + args[0] + "/WDIDataset/Indicators.csv");

        //TEST RUN FOR MERCHANDISE EXPORTS (TX.VAL.MRCH.CD.WT)
        //CO2 EN.ATM.CO2E.PC

        String[] indicators = { "TX.VAL.MRCH.CD.WT", "TM.VAL.MRCH.CD.WT", "TG.VAL.TOTL.GD.ZS", "NY.GDP.PCAP.CD", "NY.GNP.MKTP.CD"};
        String[] indicatorNames = { "Merchandise Exports $US", "Merchandise Imports $US", "Merchandise Trade % of GDP", "GDP per capita $US", "GNI $US"};
        String CO2IndicatorCode = "EN.ATM.CO2E.PC";

        //set up CO2 RDD
        JavaRDD<String> filterByCO2 = filterByIndicatorCode(stringJavaRDD, CO2IndicatorCode);
        JavaPairRDD<String, Double> pairCO2 = pair(filterByCO2);

        JavaRDD<String> initialFilter = filterByCodes(stringJavaRDD, indicators);

        // Filter out everything except the entries with the correct Indicator Code
        List<List<Tuple2<String, Tuple2<Double, Double>>>> RDDs = new ArrayList<>(5);

        for (int i = 0; i < indicators.length; i++) {

            //Filter out everything except one code
            JavaRDD<String> filtered = filterByIndicatorCode(initialFilter, indicators[i]);

            //Map the RDD to KEY VALUE pair
            JavaPairRDD<String, Double> paired = pair(filtered);

            //JOIN the RDD to the CO2 RDD
            JavaPairRDD<String, Tuple2<Double, Double>> joined = paired.join(pairCO2);

            //Collect the output
            RDDs.add(i, joined.collect());
        }
        
        for (int i = 0; i < RDDs.size(); i++) {
        	List<Tuple2<String, Tuple2<Double, Double>>> collection = RDDs.get(i);
        	System.out.println("\n" + indicatorNames[i]);
        	for (int j = 0; j < 10; j++) {
                Tuple2<String, Tuple2<Double, Double>> tuple = collection.get(j);
                String tupleString = String.format("%20.5f , %10.5f", tuple._2._1, tuple._2._2);
                System.out.println(tuple._1 + " ( " + tupleString + " )");   
        	}	
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

    private static JavaRDD<String> filterByIndicatorCode(JavaRDD rdd, String indicatorCode) {
        JavaRDD<String> ret = rdd.filter((Function<String, Boolean>) line -> {

            //split the line into tokens
            String[] tokens = line.split(",", -1);

            //If the token has a value, return true if it has the correct code
            return tokens[3] != null && tokens[3].equals(indicatorCode);
        });

        return ret;
    }

    private static JavaRDD<String> filterByCodes(JavaRDD rdd, String[] codes) {
        JavaRDD<String> ret = rdd.filter((Function<String, Boolean>) line -> {

            //split the line into tokens
            String[] tokens = line.split(",", -1);

            //If the token has a value, return true if it has the correct code
            boolean contains = false;
            for (String code : codes) {
                if (tokens[3].equals(code))
                    contains = true;
            }

            return tokens[3] != null && contains;
        });

        return ret;
    }
}