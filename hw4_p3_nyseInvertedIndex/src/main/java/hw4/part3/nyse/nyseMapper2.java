package hw4.part3.nyse;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class nyseMapper2 extends Mapper<Object, Text, Text, Text> {

//		private final static FloatWritable price  = new FloatWritable();
	       
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
	            String[] fields = value.toString().split("\t");
	            
	            Text symbol = new Text(fields[0]);
                Text level  = new Text(fields[1]);
	            
                context.write(level, symbol);
	    }
}