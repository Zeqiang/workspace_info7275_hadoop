package hw1.part4.gender;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class countMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one  = new IntWritable(1);
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
	            String[] fields = value.toString().split("::");
	            
//	            if(fields[0].contains("NYSE")){
	                Text t1 = new Text(fields[1]);
	                context.write(t1, one);
//	            }
	    }
}