package com;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HierarchyMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String movieTitle = null;
		String genres = null;
		
		String[] separatedInput = value.toString().split(",");
		
		if (separatedInput[0].equals("movieId")) {
			return;
		}
		
		if(separatedInput[1].substring(0, 1).contains("\"")) {
			String[] separatedInput2 = value.toString().split("\"");
			movieTitle = separatedInput2[1].trim();
			genres = separatedInput2[separatedInput2.length - 1].trim().substring(1);
//			System.err.println(value);
		}else {
			movieTitle = separatedInput[1].trim();
			genres = separatedInput[2].trim();
		}
		
		outkey.set(movieTitle);
		outvalue.set(genres);
		
		context.write(outkey, outvalue);
	}
}
