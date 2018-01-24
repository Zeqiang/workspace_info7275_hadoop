package hw2.part5.topMovies;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class avgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	private DoubleWritable result = new DoubleWritable();
	
	public void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		
		int count = 0;
        double sum = 0;
        
        for(DoubleWritable value :values){
            try {
                count += 1;
                sum += value.get();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
        
        result.set(sum/count);
        context.write(key, result);
        
	}
}