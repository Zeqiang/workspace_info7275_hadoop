package com.p3.cf;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class countMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] fields = value.toString().split(",");
			
	        if (fields[0].equals("NYSE") && fields.length == 9) {
	        	
	            Text symbom = new Text(fields[1]);
	            DoubleWritable price = new DoubleWritable(Double.parseDouble(fields[4]));
	            context.write(symbom, price);
	        }
	    }
}