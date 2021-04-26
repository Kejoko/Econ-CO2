package wdi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SparkHelloWorld {
    //First value will be CO2, then the rest of the indicators in order
    static List<Tuple2<String, Double>> maximums = new ArrayList<>();
    static List<Tuple2<String, Double>> minimums = new ArrayList<>();
    
    public static void main(String[] args) throws IOException {
    	SparkConf sparkConf = new SparkConf().setAppName("Spark Hello World").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("file://" + args[0] + "/WDIDataset/Indicators.csv");

        //TEST RUN FOR MERCHANDISE EXPORTS (TX.VAL.MRCH.CD.WT)
        //CO2 EN.ATM.CO2E.PC

        String[] indicators = { "TX.VAL.MRCH.CD.WT", "TM.VAL.MRCH.CD.WT", "TG.VAL.TOTL.GD.ZS", "NY.GDP.PCAP.CD", "NY.GNP.MKTP.CD"};
        String[] indicatorNames = { "Merchandise Exports $US", "Merchandise Imports $US", "Merchandise Trade % of GDP", "GDP per capita $US", "GNI $US"};
        String CO2IndicatorCode = "EN.ATM.CO2E.PC";

        // Get the CO2 data and sort it
        JavaRDD<String> filterByCO2 = filterByIndicatorCode(stringJavaRDD, CO2IndicatorCode);
        JavaPairRDD<String, Double> pairCO2 = pair(filterByCO2);
        JavaPairRDD<String, Double> sortedCO2 = sortByValue(pairCO2);
        
        // Transform the CO2 data and calculate the relevant information
        JavaPairRDD<String, Double> normalizedCO2 = featureScale(sortedCO2);
//        JavaPairRDD<String, Double> featureScaledCO2 = featureScale(sortedCO2);
//        JavaPairRDD<String, Double> normalizedCO2 = removeOutliers(normallyDistribute(sortedCO2));
        double[] co2Info = calculateMean(normalizedCO2);

        //Filter by all relevant codes
        JavaRDD<String> initialFilter = filterByCodes(stringJavaRDD, indicators);

        // Filter out everything except the entries with the correct Indicator Code
        List<List<Tuple2<String, Tuple2<Double, Double>>>> data = new ArrayList<>(5);

        double[][] meanInfo = new double[indicators.length][3];
        double[] corrCoeffs = new double[indicators.length];
        for (int i = 0; i < indicators.length; i++) {

            //Filter out everything except one code
            JavaRDD<String> filtered = filterByIndicatorCode(initialFilter, indicators[i]);

            //Map the RDD to KEY VALUE pair
            JavaPairRDD<String, Double> paired = pair(filtered);
            JavaPairRDD<String, Double> sorted = sortByValue(paired);

            //Normalize the data
            JavaPairRDD<String, Double> normalized = featureScale(sorted);
//            JavaPairRDD<String, Double> featureScaled = featureScale(sorted);
//            JavaPairRDD<String, Double> normalized = removeOutliers(normallyDistribute(sorted));
            meanInfo[i] = calculateMean(normalized);

            //JOIN the RDD to the CO2 RDD
            JavaPairRDD<String, Tuple2<Double, Double>> joined = normalized.join(normalizedCO2);

            //Collect the output
            data.add(i, joined.collect());

            //calculate coefficient
            corrCoeffs[i] = calculateCorrelationCoefficient(joined, co2Info[2], meanInfo[i][2]);
        }
        
        System.out.println("CO2 emissions metric tons per capita");
        System.out.println("Min:   " + minimums.get(0)._2);
        System.out.println("Max:   " + maximums.get(0)._2);
    	System.out.println("Count: " + co2Info[0]);
    	System.out.println("Sum:   " + co2Info[1]);
    	System.out.println("Mean:  " + co2Info[2]);
        
        for (int i = 0; i < data.size(); i++) {
        	List<Tuple2<String, Tuple2<Double, Double>>> collection = data.get(i);
        	System.out.println("\n" + indicatorNames[i]);
            System.out.println("Min:   " + minimums.get(i + 1)._2);
            System.out.println("Max:   " + maximums.get(i + 1)._2);
        	System.out.println("Count: " + meanInfo[i][0]);
        	System.out.println("Sum:   " + meanInfo[i][1]);
        	System.out.println("Mean:  " + meanInfo[i][2]);
        	System.out.println("Correlation Coefficient: " + corrCoeffs[i]);
        	for (int j = 0; j < 10; j++) {
                Tuple2<String, Tuple2<Double, Double>> tuple = collection.get(j);
                String tupleString = String.format("%20.5f , %8.7f", tuple._2._1, tuple._2._2);
                System.out.println(tuple._1 + " ( " + tupleString + " )");
        	}
        }
    }

    //Method which calls MapToPair and returns an RDD with a key of Country Code and a Value
    private static JavaPairRDD<String, Double> pair(JavaRDD<String> RDD) {
        JavaPairRDD<String, Double> paired = RDD.mapToPair((PairFunction<String, String, Double>) line -> {

            //split the line into tokens
            String[] tokens = line.split(",", -1);

            //Get the country code
            String countryCode = tokens[1];
            String year = tokens[4];
            
            if (countryCode == null || countryCode.length() == 0) {
                countryCode = "BadCode";
            }
            
            if (year == null || year.length() == 0) {
                year = "BadYear";
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

            return new Tuple2<>(key, value);
        });

        JavaPairRDD<String, Double> filtered = paired.filter((Function<Tuple2<String, Double>, Boolean>) data -> {
            return !data._1.contains("BadCode") && !data._1.contains("BadYear");
        });

        return filtered;
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
    
    private static JavaPairRDD<String, Double> sortByValue(JavaPairRDD<String, Double> rdd) {
    	// Swap the key and value in rdd
    	JavaPairRDD<Double, String> swapped = rdd.mapToPair(
    			new PairFunction<Tuple2<String, Double>, Double, String>() {
		            @Override
		            public Tuple2<Double, String> call(Tuple2<String, Double> item) throws Exception {
		                return item.swap();
		            }
    			});
    	
    	// Sort the swapped rdd by key
    	JavaPairRDD<Double, String> swappedSorted = swapped.sortByKey(true);
    	
    	// Swap it back
    	JavaPairRDD<String, Double> sorted = swappedSorted.mapToPair(
    			new PairFunction<Tuple2<Double, String>, String, Double>() {
    				@Override
    				public Tuple2<String, Double> call(Tuple2<Double, String> item) throws Exception {
    					return item.swap();
    				}
    			});
    	
    	return sorted;
    }

    private static JavaPairRDD<String, Double> featureScale(JavaPairRDD<String, Double> paired) {
        Tuple2<String, Double> maxVal = paired.max(new compareTuple());
        Tuple2<String, Double> minVal = paired.min(new compareTuple());

        maximums.add(maxVal);
        minimums.add(minVal);

        Double denominator = maxVal._2 - minVal._2;

        JavaPairRDD<String, Double> ret = paired.mapToPair((PairFunction<Tuple2<String, Double>, String, Double>) data -> {

            Double norm = (data._2 - minVal._2) / denominator;

            return new Tuple2<>(data._1, norm);
        });

        return ret;
    }
    
    private static JavaPairRDD<String, Double> normallyDistribute(JavaPairRDD<String, Double> rdd) {
    	
    	
    	
    	return rdd;
    }
    
    private static JavaPairRDD<String, Double> removeOutliers(JavaPairRDD<String, Double> rdd) {
    	// Use Interquartile Range (IQR) to identify and remove outliers
    	
    	// Find 25th percentile (Q1)
    	
    	// Find 75th percentile (Q3)
    	
    	// Find the midspread (difference between 75th and 25th percentiles)
    	// IQR = Q3 - Q1
    	
    	// Outlier if:
    	// value < Q1 - 1.5 * IQR
    	// or
    	// value > Q3 + 1.5 * IQR
    	
    	return rdd;
    }
    
    private static double[] calculateMean(JavaPairRDD<String, Double> rdd) {
    	double count = 0.0;
    	double sum = 0.0;
    	
    	List<Tuple2<String, Double>> list = rdd.collect();
    	
    	for (Tuple2<String, Double> tuple : list) {
    		count += 1.0;
    		sum += tuple._2;
    	}
    	
    	double mean = sum / count;
    	
    	double[] res = new double[3];
    	res[0] = count;
    	res[1] = sum;
    	res[2] = mean;
    	return res;
    }
    
    private static double calculateCorrelationCoefficient(JavaPairRDD<String, Tuple2<Double, Double>> rdd, double co2Mean, double econMean) {
    	List<Tuple2<String, Tuple2<Double, Double>>> list = rdd.collect();
    	
    	double numeratorSum = 0.0;
    	double denomSumX = 0.0;
    	double denomSumY = 0.0;
    	
    	for (Tuple2<String, Tuple2<Double, Double>> bigTuple : list) {
    		double x = bigTuple._2._1;
    		double y = bigTuple._2._2;
    		double diffX = x - econMean;
    		double diffY = y - co2Mean;
    		
    		numeratorSum += diffX * diffY;
    		denomSumX += diffX * diffX;
    		denomSumY += diffY * diffY;
    	}
    	
    	return numeratorSum / (Math.sqrt(denomSumX) * Math.sqrt(denomSumY));
    }
}

class compareTuple implements Serializable, Comparator<Tuple2<String, Double>> {
    @Override
    public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
        return Double.compare(o1._2(), o2._2());
    }
}