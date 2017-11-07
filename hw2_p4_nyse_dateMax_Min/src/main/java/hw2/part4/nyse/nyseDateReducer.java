package hw2.part4.nyse;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class nyseDateReducer extends Reducer<Text, CompositeValueWritable, Text, CompositeValueWritable> {
	
	public void reduce(Text key,Iterable<CompositeValueWritable> values,Context context) throws IOException, InterruptedException {
		
		String date_max = null;
		String date_min = null;
		int volumn_max = Integer.MIN_VALUE;
		int volumn_min = Integer.MAX_VALUE;
		
		for (CompositeValueWritable val : values) {
			
			if(Integer.valueOf(val.getVolume()) > volumn_max) {
				volumn_max = Integer.valueOf(val.getVolume());
				date_max =val.getDate();
			}
			
			if(Integer.valueOf(val.getVolume()) < volumn_min) {
				volumn_min = Integer.valueOf(val.getVolume());
				date_min =val.getDate();
			}
		}
		
		CompositeValueWritable volumn_date_max = new CompositeValueWritable(String.valueOf(volumn_max),date_max);
		CompositeValueWritable volumn_date_min = new CompositeValueWritable(String.valueOf(volumn_min),date_min);
		
		context.write(key, volumn_date_min);
		context.write(key, volumn_date_max);
	}
}