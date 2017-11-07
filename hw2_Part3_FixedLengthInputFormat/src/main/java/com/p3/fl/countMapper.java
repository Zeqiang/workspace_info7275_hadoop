package com.p3.fl;


import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class countMapper extends Mapper<LongWritable, BytesWritable, LongWritable, Text> {
	       
		public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			
			Text t = new Text(value.toString());
			context.write(key, t);
	    }
}