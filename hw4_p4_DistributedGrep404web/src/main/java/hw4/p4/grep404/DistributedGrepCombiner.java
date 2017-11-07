package hw4.p4.grep404;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistributedGrepCombiner extends Reducer<Text, Text, Text, Text>{
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
		boolean ifContained = false;
		String regex = context.getConfiguration().get("Regex");
		
		for(Text val : values) {
			
			if(val.toString().contains(regex)) {
				ifContained = true;
//				System.err.println("Combiner: " + key.toString());
			}
		}
		
//		System.err.println("Combiner: " + key.toString());
		if(ifContained == true) {
			
			context.write(key, new Text(regex));
		}
		
	}
}
