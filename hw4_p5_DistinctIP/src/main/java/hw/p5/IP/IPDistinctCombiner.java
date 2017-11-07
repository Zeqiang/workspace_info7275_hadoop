package hw.p5.IP;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IPDistinctCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {
    
	public void reduce(Text key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {

//		System.err.println("Combiner: " + String.valueOf(key));
		context.write(key, NullWritable.get());
	}
}