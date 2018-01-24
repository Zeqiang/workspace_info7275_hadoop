package com.p3.kv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class countReducer extends Reducer<Text, Text, Text, Text> {

//	private IntWritable total = new IntWritable();
    
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
//		int sum = 0;
//		
//		for (IntWritable val : values) {
//			sum +=val.get();
//		}
//		
//		total.set(sum);
//		context.write(key, total);
		
		for (Text val : values) {
			context.write(key, val);
		}
	}
}