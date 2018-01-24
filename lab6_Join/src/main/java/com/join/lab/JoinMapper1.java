package com.join.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper1 extends Mapper<Object, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] separatedInput = value.toString().split(",");

		String questionID = separatedInput[0].trim().replaceAll("\"", "");

		if (questionID == null) {
			return;
		}
		
		outKey.set(questionID);
		outValue.set("A" + value);

		if (!questionID.equals("Question ID")) {
			context.write(outKey, outValue);
		}

	}

}