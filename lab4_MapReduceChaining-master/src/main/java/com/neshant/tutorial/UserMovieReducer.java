package com.neshant.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class UserMovieReducer extends
        Reducer<Text, FloatWritable, Text, FloatWritable> {
	

 
	  public void reduce(Text text, Iterable<FloatWritable> values, Context context)
	            throws IOException, InterruptedException {
	        Float sum = 0.0f;
	        Float count =0.0f;
	        for (FloatWritable value : values) {
	            sum += value.get();
	            count=count+1;
	        }
	        
	         
	         Float average= (float) (sum/count);
	        
	        context.write(text, new FloatWritable(average));
	    }
}