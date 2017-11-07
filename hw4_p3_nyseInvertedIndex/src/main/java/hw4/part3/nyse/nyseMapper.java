package hw4.part3.nyse;


import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class nyseMapper extends Mapper<LongWritable, Text, Text, Text> {
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
	            String[] fields = value.toString().split(",");
	            
	            if(fields[0].contains("NYSE")){
	            	
	                Text t1 = new Text(fields[1]);
	                Text price  = new Text(fields[8]);
	                context.write(t1, price);
	            }
	    }
}