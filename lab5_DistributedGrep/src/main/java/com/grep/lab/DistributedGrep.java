package com.grep.lab;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedGrep {

	public static class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {

		// Pattern p = Pattern.compile("[a-z]");
		// Matcher m = p.matcher(inputstring);
		// if (m.find())

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String txt = value.toString();
			String mapRegex = context.getConfiguration().get("mapregex");

			if (txt.contains(mapRegex)) {
				context.write(NullWritable.get(), value);
			}
		}
	}

}