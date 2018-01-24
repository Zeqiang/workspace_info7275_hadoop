package hw1.part6.IPweb;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IPwebReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable total = new IntWritable();
    
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		
		for (IntWritable val : values) {
			sum +=val.get();
		}
		
		total.set(sum);
		
		Text tt = new Text(key + ": ");
		context.write(tt, total);
	}
}