package hw5.p3.bin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    
	public void reduce(Text key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
	
		int sum = 0;
		
		for(NullWritable val : values) {
			sum += 1;
		}
		
		Text result = new Text(key.toString() + ": " + String.valueOf(sum));
		context.write(result, NullWritable.get());
	}
}