package com.p3.fl;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class countReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private DoubleWritable average = new DoubleWritable();
    
	public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		
		double sum = 0;
        int count = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count ++;
        }
        average.set(sum/count);
        context.write(key, average);
	}
}