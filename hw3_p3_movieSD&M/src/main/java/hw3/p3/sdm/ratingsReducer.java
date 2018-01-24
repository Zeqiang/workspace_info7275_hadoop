package hw3.p3.sdm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ratingsReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, MedianStdDevWritable> {
	
	MedianStdDevWritable result = new MedianStdDevWritable();
	ArrayList<Double> ratingList = new ArrayList<Double>();
	
	public void reduce(IntWritable key,Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		
		double sum = 0.0;
		int count = 0;
		ratingList.clear();
		
		// calculate sum & count; add ratings into arraylist
		for (DoubleWritable val : values) {
			
			ratingList.add(val.get());
			sum += val.get();
			count += 1;
		}
		
		// sort arraylist
		Collections.sort(ratingList);
		
		// calculate median
		double median;
		if(count % 2 == 0) {
			median = (ratingList.get(count/2 - 1) + ratingList.get(count/2)) / 2;
			result.setMedian(String.valueOf(median));
		}else {
			median = ratingList.get(count/2);
			result.setMedian(String.valueOf(median));
		}
		
		// calculate standard deviation
		double mean = sum /count;
		double sumOfSquares = 0.0;
		for(Double d : ratingList) {
			sumOfSquares += (d -mean) * (d -mean);
		}
		double stdDev = Math.sqrt(sumOfSquares/(count -1));
		result.setStdDev(String.valueOf(stdDev));
		
		context.write(key, result);
	}
}