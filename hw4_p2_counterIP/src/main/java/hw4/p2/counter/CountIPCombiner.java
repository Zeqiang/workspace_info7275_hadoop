package hw4.p2.counter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountIPCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable result = new IntWritable();
	public static final String IP_COUNTER_GROUP = "IP_Counter";
    
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

		int sum = 0;
		
		for(IntWritable val : values) {
			
			sum += val.get();
			context.getCounter(IP_COUNTER_GROUP, String.valueOf(key)).increment(1);
		}
		
		result.set(sum);
		
		Text t = new Text();
		t.set(key + ": ");
//		System.err.println("Combiner: " + String.valueOf(key));
		context.write(t, result);
		
//		context.getCounter(IP_COUNTER_GROUP, "Num of access").increment(1);
	}
}