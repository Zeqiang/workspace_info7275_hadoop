package com.neshant.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, FloatWritable, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException

	{

		
		String[] fields = value.toString().split("\t");

		Text grouper = new Text(fields[0]);
		FloatWritable rating = new FloatWritable(Float.parseFloat(fields[1]));

		context.write(rating, grouper);

	}

}
