package hw5.p2.partition;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MonthReducer extends Reducer<Text, Text, Text, NullWritable> {
    
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
		for(Text val : values) {
			context.write(val, NullWritable.get());
		}
	}
}