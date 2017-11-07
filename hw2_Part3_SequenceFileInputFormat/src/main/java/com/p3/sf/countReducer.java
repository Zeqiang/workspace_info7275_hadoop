package com.p3.sf;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class countReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        for (LongWritable val : values) {
        		context.write(key, val);
        }
	}
}