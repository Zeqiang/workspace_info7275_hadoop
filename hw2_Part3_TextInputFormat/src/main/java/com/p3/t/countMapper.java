package com.p3.t;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class countMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one  = new IntWritable(1);
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] values = value.toString().split(" ");
			
			if(values[0].contains("[")) {
				
				Text t1 = new Text(values[5]);
				context.write(t1, one);
			}
	    }
}