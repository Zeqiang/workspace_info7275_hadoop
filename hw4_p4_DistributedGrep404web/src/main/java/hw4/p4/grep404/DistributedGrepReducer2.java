package hw4.p4.grep404;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistributedGrepReducer2 extends Reducer<Text, NullWritable, Text, NullWritable>{
	
	public void reduce(Text key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
		
		context.write(key, NullWritable.get());
	}
}
