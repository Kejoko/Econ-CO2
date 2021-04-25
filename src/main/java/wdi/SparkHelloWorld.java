package wdi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SparkHelloWorld {

	static int co2Count = 0;
	static int counts[] = {0, 0, 0, 0, 0};
	static double co2Sum = 0;
	static double sums[] = {0, 0, 0, 0, 0};
	static double co2Mean = 0;
	static double means[] = {0, 0, 0, 0, 0};
	
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
        JavaPairRDD<String, Double> pairCO2 = pair(-1, filterByCO2);
        co2Mean = co2Sum / co2Count;
        //normalize values first so that join already has normalized values
        JavaPairRDD<String, Double> normalizedCO2 = normalize(pairCO2);

        //Filter by all relevant codes
        JavaRDD<String> initialFilter = filterByCodes(stringJavaRDD, indicators);

        // Filter out everything except the entries with the correct Indicator Code
        List<List<Tuple2<String, Tuple2<Double, Double>>>> data = new ArrayList<>(5);

        for (int i = 0; i < indicators.length; i++) {

            //Filter out everything except one code
            JavaRDD<String> filtered = filterByIndicatorCode(initialFilter, indicators[i]);

            //Map the RDD to KEY VALUE pair
            JavaPairRDD<String, Double> paired = pair(i, filtered);
            means[i] = sums[i] / counts[i];

            //Normalize the data
            JavaPairRDD<String, Double> normalized = normalize(paired);

            //JOIN the RDD to the CO2 RDD
            JavaPairRDD<String, Tuple2<Double, Double>> joined = normalized.join(normalizedCO2);

            //Collect the output
            data.add(i, joined.collect());

            //calculate coefficient
            //pass collection to function... put coefficient on array or something
            double correlationCoefficient = calculateCorrelationCoefficient(joined);

        }
        
        System.out.println("CO2 emissions metric tons per capita");
        System.out.println("Count: " + co2Count);
        System.out.println("Sum: " + co2Sum);
        System.out.println("Mean: " + co2Mean);
        
        for (int i = 0; i < data.size(); i++) {
        	List<Tuple2<String, Tuple2<Double, Double>>> collection = data.get(i);
        	System.out.println("\n" + indicatorNames[i]);
        	System.out.println("Count: " + counts[i]);
        	System.out.println("Sum: " + sums[i]);
        	System.out.println("Mean: " + means[i]);
        	for (int j = 0; j < 10; j++) {
                Tuple2<String, Tuple2<Double, Double>> tuple = collection.get(j);
                String tupleString = String.format("%8.7f , %8.7f", tuple._2._1, tuple._2._2);
                System.out.println(tuple._1 + " ( " + tupleString + " )");
        	}
        }
    }

    //Method which calls MapToPair and returns an RDD with a key of Country Code and a Value
    private static JavaPairRDD<String, Double> pair(int indicatorIndex, JavaRDD<String> RDD) {
        return RDD.mapToPair((PairFunction<String, String, Double>) line -> {

            //split the line into tokens
            String[] tokens = line.split(",", -1);

            //Get the country code
            String countryCode = tokens[1];
            String year = tokens[4];
            boolean shouldUpdate = true;
            if (countryCode == null || countryCode.length() == 0) {
                countryCode = "BadCode";
                shouldUpdate = false;
            }
            if (year == null || year.length() == 0) {
                year = "BadYear";
                shouldUpdate = false;
            }

            String key = countryCode + year;

            Double value;
            //Get the value
            try {
                value = Double.parseDouble(tokens[5]);
            }
            catch (NumberFormatException e)
            {
                value = 0.0;
            }
            
            if (shouldUpdate) {
            	if (indicatorIndex == -1) {
            		co2Sum += value;
            		co2Count++;
            	} else {
            		sums[indicatorIndex] += value;
            		counts[indicatorIndex]++;
            	}
            }

            return new Tuple2<>(key, value);
        });
        
        // Filter out the bad codes
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

    private static JavaPairRDD<String, Double> normalize(JavaPairRDD<String, Double> paired) {
        Tuple2<String, Double> maxVal = paired.max(new compareTuple());
        Tuple2<String, Double> minVal = paired.min(new compareTuple());

        Double denominator = maxVal._2 - minVal._2;

        JavaPairRDD<String, Double> ret = paired.mapToPair((PairFunction<Tuple2<String, Double>, String, Double>) data -> {

            Double norm = (data._2 - minVal._2) / denominator;

            return new Tuple2<>(data._1, norm);
        });

        return ret;
    }
    
    private static double calculateCorrelationCoefficient(JavaPairRDD<String, Tuple2<Double, Double>> rdd) {
    	// Calculate xmean
    	
    	// Calulate ymean
    	
    	// Where x is economic value
    	// Where y is co2 value
    	// Sum for all i: (xi - xmean)(yi - ymean)
    	
    	
    	return 0;
    }
}

class compareTuple implements Serializable, Comparator<Tuple2<String, Double>> {
    @Override
    public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
        return Double.compare(o1._2(), o2._2());
    }
}