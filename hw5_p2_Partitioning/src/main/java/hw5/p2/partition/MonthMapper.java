package hw5.p2.partition;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MonthMapper extends Mapper<LongWritable, Text, Text, Text> {
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
	            String[] ip = value.toString().split(" ");
	            String[] times = ip[3].toString().split("/");

	            Text time = new Text(times[1]);
	            
	            context.write(time, value);
	            
//	            System.err.println("Mapper: " + String.valueOf(t1));
	    }
}