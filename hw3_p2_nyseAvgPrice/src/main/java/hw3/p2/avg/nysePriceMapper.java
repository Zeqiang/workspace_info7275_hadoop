package hw3.p2.avg;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class nysePriceMapper extends Mapper<LongWritable, Text, Text, CountAvgWritable> {
	
	private CountAvgWritable output = new CountAvgWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split(",");
		
		if(values[0].contains("NYSE")) {
			
//			System.err.println(values[2]);
			
			String years[] = values[2].toString().split("-");
			
			String symbol = values[1];
			String price = values[8];
			String year = years[0];
			
//			System.err.println(years[2]);
			
			Text symYear = new Text();
			symYear.set(symbol + "-" + year + ": ");
			
			output.setAverage(Double.parseDouble(price));
			output.setCount(1);
			
			context.write(symYear, output);
		}
	}
}