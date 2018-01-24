package hw2.part5.topMovies;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class sortMapper extends Mapper<Object, Text, CompositeKeyWritable, NullWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split("\t");
		
        String movieID = null;
        String rating = null;
        
        try {
            movieID = values[0];
            rating = values[1];
        } catch (Exception e) {
            // TODO: handle exception
        }

        if (movieID != null && rating != null) {
        		
        		CompositeKeyWritable ck = new CompositeKeyWritable(movieID, rating);
            
        		try {
                context.write(ck, NullWritable.get());
            } catch (Exception e) {
            		// TODO: handle exception
                System.out.println("sortMapper error");
            }
        }
        
	}
}