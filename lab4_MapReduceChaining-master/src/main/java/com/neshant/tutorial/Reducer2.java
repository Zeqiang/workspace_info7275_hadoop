package com.neshant.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<FloatWritable, Text,Text,FloatWritable> {
	int count = 0;
	public void reduce(FloatWritable floatWritable, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		for (Text value : values) {
			count++;
			if (count <= 25) {
				context.write(value,floatWritable);	
				
			}else {return;}
			
			
		}
		
	}

}
