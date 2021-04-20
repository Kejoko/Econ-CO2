package wdi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.spark.*;

import wdi.util.*;

public class WDImain {

	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];
		
		try {
			Configuration conf1 = new Configuration();
			
			Job job1 = Job.getInstance(conf1, "Q1 & Q2 - Best/Worst Days of the Week");
			job1.setJarByClass(WDImain.class);
			job1.setMapperClass(DummyMapper.class);
			job1.setCombinerClass(DummyReducer.class);
			job1.setReducerClass(DummyReducer.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(DoubleWritable.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(DoubleWritable.class);
			
			for (String inputFile : Inputs.mainFiles) {
				FileInputFormat.addInputPath(job1, new Path(inputPath + inputFile));
			}
			FileOutputFormat.setOutputPath(job1, new Path(outputPath));
			
			job1.waitForCompletion(true);
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		}
	}

}
