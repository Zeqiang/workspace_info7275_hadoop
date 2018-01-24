package hw3.p2.avg;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class nysePriceReducer extends Reducer<Text, CountAvgWritable, Text, CountAvgWritable> {
	
	public void reduce(Text key,Iterable<CountAvgWritable> values,Context context) throws IOException, InterruptedException {
		
		for (CountAvgWritable val : values) {
			
//			System.err.println("reducer: ---" + String.valueOf(val.getCount()) + "---" + String.valueOf(val.getAverage()));
			context.write(key, val);
		}
	}
}