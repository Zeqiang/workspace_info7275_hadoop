package hw2.part4.nyse;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class nysePriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split(",");
		
		if(values[0].contains("NYSE")) {
			
			String symbol = values[1];
			String price = values[8];
			
			Text sym = new Text();
			sym.set(symbol);
			DoubleWritable pri = new  DoubleWritable();
			pri.set(Double.parseDouble(price));
			
			context.write(sym, pri);
		}
	}
}