package com.p3.nl;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class countReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (Text val : values) {
            context.write(key, val);
        }
	}
}