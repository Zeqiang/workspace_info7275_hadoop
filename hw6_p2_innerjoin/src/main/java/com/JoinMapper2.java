package com;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper2 extends Mapper<Object, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] separatedInput = value.toString().split(";");

		String userID = separatedInput[0].trim().replaceAll("\"", "");

		if (userID == null) {
			return;
		}
		
		outKey.set(userID);
		outValue.set("B" + value);

		if (!userID.equals("User-ID")) {
			context.write(outKey, outValue);
		}
		
	}
}