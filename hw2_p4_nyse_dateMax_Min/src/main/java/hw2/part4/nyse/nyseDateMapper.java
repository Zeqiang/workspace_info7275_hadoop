package hw2.part4.nyse;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class nyseDateMapper extends Mapper<LongWritable, Text, Text, CompositeValueWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split(",");
		
		if(values[0].contains("NYSE")) {
			
			String symbol = values[1];
			String date = values[2];
			String volume = values[7];
			
			CompositeValueWritable cw = new CompositeValueWritable(date, volume);
			
			Text sym = new Text();
			sym.set(symbol);
			
			context.write(sym, cw);
		}
	}
}