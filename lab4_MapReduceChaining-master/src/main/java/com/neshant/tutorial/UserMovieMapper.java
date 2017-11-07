package com.neshant.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class UserMovieMapper extends Mapper<Object, Text, Text, FloatWritable> {

	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		LongWritable a = (LongWritable) key;
		
        if (a.get() == 0 && value.toString().contains("exchange")) {
        	
        		return;
        }else {
            	String[] fields = value.toString().split("::");
        		
        		FloatWritable rating = new FloatWritable(Float.parseFloat(fields[2]));
        		Text grouper = new Text(fields[1]);
        		
        		context.write(grouper, rating);
            }
        }
}