package hw2.part4.nyse;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class nysePriceReducer extends Reducer<Text, DoubleWritable, CompositeKeyWritable, NullWritable> {
	
	public void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		
		Double price_max = 0.0;
		
		for (DoubleWritable val : values) {
			
			if(val.get() > price_max) {
				price_max =val.get();
			}
		}
		
		CompositeKeyWritable cw = new CompositeKeyWritable(String.valueOf(key),String.valueOf(price_max));
		
		context.write(cw, NullWritable.get());
	}
}