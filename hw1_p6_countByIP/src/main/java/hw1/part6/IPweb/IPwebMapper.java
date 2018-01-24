package hw1.part6.IPweb;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IPwebMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one  = new IntWritable(1);
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
	            String[] ip = value.toString().split(" ");

	            Text t1 = new Text(ip[0]);
	            context.write(t1, one);
	    }
}