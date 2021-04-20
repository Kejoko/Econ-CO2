package wdi;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DummyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Skip the header line defining all of the columns
		if (key.get() == 0) return;
		
		// Parse the csv into values
		String line = value.toString();
		String[] values = line.split(",");
		
		for (String val : values) {
			context.write(new Text(val), new DoubleWritable(1));
		}
	}

}
