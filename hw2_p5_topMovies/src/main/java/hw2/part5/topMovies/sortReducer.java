package hw2.part5.topMovies;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
 
public class sortReducer extends Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {
	
	private static int count = 0;
	
	public void reduce(CompositeKeyWritable key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
		
		for(NullWritable value : values){
			
			if (count < 25) {
	            context.write(key, NullWritable.get());
	            count ++;
	        }
			
        }
	}
}