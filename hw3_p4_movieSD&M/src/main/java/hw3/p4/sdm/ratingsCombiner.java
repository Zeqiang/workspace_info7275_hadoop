package hw3.p4.sdm;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ratingsCombiner extends Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable> {
	
	public void reduce(IntWritable key,Iterable<SortedMapWritable> values,Context context) throws IOException, InterruptedException {
		
		SortedMapWritable output = new SortedMapWritable();
		
		for(SortedMapWritable val : values) {
			
			for(Entry<WritableComparable, Writable> entry : val.entrySet()) {
				
				IntWritable count = (IntWritable) output.get(entry.getKey());
				
				if(count != null) {
					
					count.set(count.get() + ((IntWritable) entry.getValue()).get());
				}else {
					output.put(entry.getKey(), new IntWritable(((IntWritable) entry.getValue()).get()));
				}
			}
			val.clear();
		}
		
//		System.err.println("2..."+key.toString()+"..."+output.keySet()+"..."+output.values());
        context.write(key, output);
	}
}



