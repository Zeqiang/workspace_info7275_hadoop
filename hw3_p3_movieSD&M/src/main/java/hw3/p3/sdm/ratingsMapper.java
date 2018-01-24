package hw3.p3.sdm;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ratingsMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
	
	IntWritable id = new IntWritable();
	DoubleWritable rating = new DoubleWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split("::");
			
		String movieID = values[1];
		String ratings = values[2];
		
		id.set(Integer.parseInt(movieID));
		rating.set(Double.parseDouble(ratings));
			
		context.write(id, rating);
	}
}