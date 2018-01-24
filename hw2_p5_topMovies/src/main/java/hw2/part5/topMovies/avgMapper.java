package hw2.part5.topMovies;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class avgMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split("::");
		
        String movie_id = null;
        double rating = 0;
        
        try {
            movie_id = values[1];
            rating = Double.parseDouble(values[2]);
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("Input Error");
        }

        if (movie_id != null) {

            try {
                context.write(new Text(movie_id), new DoubleWritable(rating));
            } catch (Exception e) {
            	// TODO: handle exception
            	System.out.print("avgMapper output error");
            }
        }
        
	}
}