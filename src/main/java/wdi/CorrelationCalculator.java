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

public class CorrelationCalculator {
    static final String[] indicators = { "TX.VAL.MRCH.CD.WT", "TM.VAL.MRCH.CD.WT", "TG.VAL.TOTL.GD.ZS", "NY.GDP.PCAP.CD", "NY.GNP.MKTP.CD"};
    static final String[] indicatorNames = { "Merchandise Exports $US", "Merchandise Imports $US", "Merchandise Trade % of GDP", "GDP per capita $US", "GNI $US"};
    static final String CO2IndicatorCode = "EN.ATM.CO2E.PC";
	
    // Minimum and maximum values in the datasets
    static double co2Max;
    static double co2Min;
    static double[] maximums = new double[indicators.length];
    static double[] minimums = new double[indicators.length];
    
    // The box plot information for each of the indicators
    static double[] co2Qs = new double[5];
    static double[][] econQs = new double[indicators.length][5];
    
    static class compareTuple implements Serializable, Comparator<Tuple2<String, Double>> {
        @Override
        public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
            return Double.compare(o1._2(), o2._2());
        }
    }
    
    public static void main(String[] args) throws IOException {
    	String homeDir = args[0];
    	boolean normalize = false;
    	if (args.length > 1 && args[1].equals("true")) normalize = true;
    	
    	SparkConf sparkConf = new SparkConf().setAppName("Economic indicators correlation to CO2 emssions").setMaster("spark://des-moines:50000");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("file://" + homeDir + "/WDIDataset/Indicators.csv");

        //TEST RUN FOR MERCHANDISE EXPORTS (TX.VAL.MRCH.CD.WT)
        //CO2 EN.ATM.CO2E.PC

        // Get the CO2 data and sort it
        JavaRDD<String> filterByCO2 = filterByIndicatorCode(stringJavaRDD, CO2IndicatorCode);
        JavaPairRDD<String, Double> pairCO2 = pair(filterByCO2);
        JavaPairRDD<String, Double> sortedCO2 = sortByValue(pairCO2);
        
        // Transform the CO2 data and calculate the relevant information
        JavaPairRDD<String, Double> trimmedCO2;
        double[] co2Info;
        if (!normalize) {
		    trimmedCO2 = removeOutliers(-1, sortedCO2);
		    co2Min = trimmedCO2.min(new compareTuple())._2;
		    co2Max = trimmedCO2.max(new compareTuple())._2;
		    co2Info = calculateMean(trimmedCO2);
        } else {
	        JavaPairRDD<String, Double> normalizedCO2 = featureScale(-1, sortedCO2);
	        trimmedCO2 = removeOutliers(-1, normalizedCO2);
	        co2Info = calculateMean(trimmedCO2);
        }

        // Filter by all relevant codes
        JavaRDD<String> initialFilter = filterByCodes(stringJavaRDD, indicators);

        // Filter out everything except the entries with the correct Indicator Code
        List<List<Tuple2<String, Tuple2<Double, Double>>>> data = new ArrayList<>(indicators.length);

        double[][] econInfo = new double[indicators.length][3];
        double[] corrCoeffs = new double[indicators.length];
        for (int i = 0; i < indicators.length; i++) {

            // Filter out everything except one code
            JavaRDD<String> filtered = filterByIndicatorCode(initialFilter, indicators[i]);

            // Map the RDD to KEY VALUE pair
            JavaPairRDD<String, Double> paired = pair(filtered);
            JavaPairRDD<String, Double> sorted = sortByValue(paired);

            // Transform the data
            JavaPairRDD<String, Double> trimmed;
            if (!normalize) {
	            trimmed = removeOutliers(i, sorted);
	            minimums[i] = trimmed.min(new compareTuple())._2;
	            maximums[i] = trimmed.max(new compareTuple())._2;
	            econInfo[i] = calculateMean(trimmed);
            } else { 
	            JavaPairRDD<String, Double> normalized = featureScale(i, sorted);
	            trimmed = removeOutliers(i, normalized);
	            econInfo[i] = calculateMean(trimmed);
            }

            // JOIN the RDD to the CO2 RDD
//            JavaPairRDD<String, Tuple2<Double, Double>> joined = normalized.join(normalizedCO2);
            JavaPairRDD<String, Tuple2<Double, Double>> joined = trimmed.join(trimmedCO2);

            // Collect the output
            data.add(i, joined.collect());

            // Calculate coefficient
            corrCoeffs[i] = calculateCorrelationCoefficient(joined, co2Info[2], econInfo[i][2]);
        }
        
        // Report all of the relevant information
        System.out.println("CO2 emissions metric tons per capita");
        System.out.println("Min:   " + String.format("%25.6f", co2Min));
        System.out.println("Low:   " + String.format("%25.6f", co2Qs[3]));
        System.out.println("Q1:    " + String.format("%25.6f", co2Qs[0]));
        System.out.println("Med:   " + String.format("%25.6f", co2Qs[1]));
        System.out.println("Q3:    " + String.format("%25.6f", co2Qs[2]));
        System.out.println("High:  " + String.format("%25.6f", co2Qs[4]));
        System.out.println("Max:   " + String.format("%25.6f", co2Max));
        System.out.println("Count: " + String.format("%25.6f", co2Info[0]));
        System.out.println("Sum:   " + String.format("%25.6f", co2Info[1]));
        System.out.println("Mean:  " + String.format("%25.6f", co2Info[2]));
        
        for (int i = 0; i < data.size(); i++) {
        	List<Tuple2<String, Tuple2<Double, Double>>> collection = data.get(i);
        	System.out.println("\n" + indicatorNames[i]);
            System.out.println("Min:   " + String.format("%25.6f", minimums[i]));
            System.out.println("Low:   " + String.format("%25.6f", econQs[i][3]));
            System.out.println("Q1:    " + String.format("%25.6f", econQs[i][0]));
            System.out.println("Med:   " + String.format("%25.6f", econQs[i][1]));
            System.out.println("Q3:    " + String.format("%25.6f", econQs[i][2]));
            System.out.println("High:  " + String.format("%25.6f", econQs[i][4]));
            System.out.println("Max:   " + String.format("%25.6f", maximums[i]));
            System.out.println("Count: " + String.format("%25.6f", econInfo[i][0]));
            System.out.println("Sum:   " + String.format("%25.6f", econInfo[i][1]));
            System.out.println("Mean:  " + String.format("%25.6f", econInfo[i][2]));
        	System.out.println("Correlation Coefficient: " + corrCoeffs[i]);
        	for (int j = 0; j < 10; j++) {
                Tuple2<String, Tuple2<Double, Double>> tuple = collection.get(j);
                String tupleString = String.format("%20.5f , %8.5f", tuple._2._1, tuple._2._2);
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
            } catch (NumberFormatException e) {
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

    private static JavaPairRDD<String, Double> featureScale(int indicatorIndex, JavaPairRDD<String, Double> paired) {
        double maxVal = paired.max(new compareTuple())._2;
        double minVal = paired.min(new compareTuple())._2;
        
        if (indicatorIndex == -1) {
        	co2Max = maxVal;
        	co2Min = minVal;
        } else {
        	maximums[indicatorIndex] = maxVal;
        	minimums[indicatorIndex] = maxVal;
        }

        Double denominator = maxVal - minVal;

        JavaPairRDD<String, Double> ret = paired.mapToPair((PairFunction<Tuple2<String, Double>, String, Double>) data -> {

            Double norm = (data._2 - minVal) / denominator;

            return new Tuple2<>(data._1, norm);
        });

        return ret;
    }
    
    private static double calculateMedian(List<Tuple2<String, Double>> list, int startIndex, int endIndex) {
    	int size = endIndex - startIndex;
    	
    	double median;
    	
    	if (size % 2 == 0) {
    		int firstMedianOffset = (size / 2) - 1;
    		int secondMedianOffset = size / 2;
    		double firstMedian = list.get(startIndex + firstMedianOffset)._2;
    		double secondMedian = list.get(startIndex + secondMedianOffset)._2;
        	
    		median = (firstMedian + secondMedian) / 2;
    	} else {
    		int medianOffset = Math.floorDiv(size, 2);
    		
    		median = list.get(startIndex + medianOffset)._2;
    	}
    	
    	return median;
    }
    
    private static JavaPairRDD<String, Double> removeOutliers(int indicatorIndex, JavaPairRDD<String, Double> rdd) {
    	List<Tuple2<String, Double>> list = rdd.collect();
    	int size = list.size();
    	
    	// Calculate Q2
    	double median = calculateMedian(list, 0, size);
    	
    	// Calculate Q1 and Q3
    	int sublistSize;
    	if (size % 2 == 0) {
    		sublistSize = (size / 2) - 1;
    	} else {
    		sublistSize = (size - 1) / 2;
    	}
    	double q1 = calculateMedian(list, 0, sublistSize);
    	double q3 = calculateMedian(list, size - sublistSize, size);
    	
    	// Calculate IQR and tolerances
    	double iqr = q3 - q1;
    	double lowTolerance = q1 - (1.5 * iqr);
    	double highTolerance = q3 + (1.5 * iqr);
    	
    	if (indicatorIndex == -1) {
    		co2Qs[0] = q1;
    		co2Qs[1] = median;
    		co2Qs[2] = q3;
    		co2Qs[3] = lowTolerance;
    		co2Qs[4] = highTolerance;
    	} else {
    		econQs[indicatorIndex][0] = q1;
    		econQs[indicatorIndex][1] = median;
    		econQs[indicatorIndex][2] = q3;
    		econQs[indicatorIndex][3] = lowTolerance;
    		econQs[indicatorIndex][4] = highTolerance;
    	}
    	
    	JavaPairRDD<String, Double> filtered = rdd.filter((Function<Tuple2<String, Double>, Boolean>) pair -> {
    		if (pair._2 < lowTolerance || pair._2 > highTolerance) {
    			return false;
    		}
    		
    		return true;
    	});
    	
    	return filtered;
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

