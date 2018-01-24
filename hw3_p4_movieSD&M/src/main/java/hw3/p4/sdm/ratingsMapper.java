package hw3.p4.sdm;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ratingsMapper extends Mapper<LongWritable, Text, IntWritable, SortedMapWritable> {
	
	IntWritable id = new IntWritable();
	DoubleWritable rating = new DoubleWritable();
	IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split("::");
			
		String movieID = values[1];
		String ratings = values[2];
		
		id.set(Integer.parseInt(movieID));
		rating.set(Double.parseDouble(ratings));
		
		SortedMapWritable ratingResult = new SortedMapWritable();
		ratingResult.put(rating, one);
		
//		System.err.println("1..."+id.toString()+"..."+ratingResult.keySet()+"..."+ratingResult.values());
		context.write(id, ratingResult);
	}
}