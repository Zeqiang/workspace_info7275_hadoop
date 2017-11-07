package hw.p5.IP;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IPDistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
	            String[] ip = value.toString().split(" ");

	            Text t1 = new Text(ip[0]);
	            context.write(t1, NullWritable.get());
	            
//	            System.err.println("Mapper: " + String.valueOf(t1));
	    }
}