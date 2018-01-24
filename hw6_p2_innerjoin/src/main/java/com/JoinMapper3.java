package com;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper3 extends Mapper<Object, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] separated = value.toString().split("\t");
		String[] separatedInput = separated[0].toString().split(";");
		
		String ISBN = separatedInput[1].trim().replaceAll("\"", "");

		if (ISBN == null) {
			return;
		}
		
		outKey.set(ISBN);
		outValue.set("C" + value);
		
		context.write(outKey, outValue);
		
	}
}