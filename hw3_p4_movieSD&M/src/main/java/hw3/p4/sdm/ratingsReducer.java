package hw3.p4.sdm;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ratingsReducer extends Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevWritable> {
	
	MedianStdDevWritable result = new MedianStdDevWritable();
	TreeMap<Double, Integer> ratingCount = new TreeMap<Double, Integer>();
	
	public void reduce(IntWritable key,Iterable<SortedMapWritable> values,Context context) throws IOException, InterruptedException {
		
		double sum = 0.0;
		int totalAmount = 0;
		ratingCount.clear();
		
		// calculate total count & sum
		// insert data into TreeMap
		for (SortedMapWritable val : values) {
			
			for(Entry<WritableComparable, Writable> entry : val.entrySet()) {
				
				Double rate = ((DoubleWritable) entry.getKey()).get();
				int count = ((IntWritable) entry.getValue()).get();
				
				totalAmount += count;
				sum += rate * count;
				
				Integer storedCount = ratingCount.get(rate);
				if(storedCount == null) {
					ratingCount.put(rate, count);
				}else {
					ratingCount.put(rate, storedCount + count);
				}
			}
			val.clear();
		}
		
//		System.err.println(totalAmount);
//		System.err.println(sum);
		
		// calculate median
		int medianIndex = totalAmount/2;
		int prevRatingCount = 0;
		int RatingCount = 0;
		double prevKey = 0.0;
		
		for(Entry<Double, Integer> entry : ratingCount.entrySet()) {
			
			RatingCount = prevRatingCount + entry.getValue();
			
			if(prevRatingCount <= medianIndex && medianIndex < RatingCount) {
				
				if(totalAmount % 2 == 0 && prevRatingCount == medianIndex) {
					double median = (double) (entry.getKey() + prevKey) / 2.0;
					result.setMedian(String.valueOf(median));
				}else {
					result.setMedian(String.valueOf(entry.getKey()));
				}
				break;
			}
			prevRatingCount = RatingCount;
			prevKey = entry.getKey();
		}
		
		// calculate StdDev
		double mean = sum/totalAmount;
		double sumOfSquare = 0.0;
		
		for(Entry<Double, Integer> entry : ratingCount.entrySet()) {
			sumOfSquare += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
		}
		double stdDev = Math.sqrt(sumOfSquare/(totalAmount - 1));
		result.setStdDev(String.valueOf(stdDev));
		
		context.write(key, result);
	}
}