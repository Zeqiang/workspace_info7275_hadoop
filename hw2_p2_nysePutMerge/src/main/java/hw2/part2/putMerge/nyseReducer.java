package hw2.part2.putMerge;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class nyseReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	private FloatWritable avg = new FloatWritable();
    
	public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
		
		float sum = 0;
		float n = 0;
		
		for (FloatWritable val : values) {
			sum +=val.get();
			n += 1;
		}
		
		float result = sum/n;
        
		avg.set(result);
		context.write(key, avg);
	}
}