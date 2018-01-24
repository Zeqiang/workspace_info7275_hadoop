package hw3.p2.avg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class nysePriceCombiner extends Reducer<Text, CountAvgWritable, Text, CountAvgWritable> {
	
	private CountAvgWritable result = new CountAvgWritable();
	
	public void reduce(Text key,Iterable<CountAvgWritable> values,Context context) throws IOException, InterruptedException {
		
		double sum = 0.0;
		long count = 0;
		
		for (CountAvgWritable val : values) {
			
			sum += val.getCount() * val.getAverage();
			count += val.getCount();
		}
		
		result.setCount(count);
		result.setAverage(sum/count);
		
//		System.err.println("combiner: ---" + String.valueOf(result.getCount()) + "---" + String.valueOf(result.getAverage()));
		
        context.write(key, result);
	}
}