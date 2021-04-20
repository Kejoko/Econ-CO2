package wdi;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DummyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		
		for (DoubleWritable val : values) {
			count += val.get();
		}
		
		context.write(key, new DoubleWritable(count));
	}
}
