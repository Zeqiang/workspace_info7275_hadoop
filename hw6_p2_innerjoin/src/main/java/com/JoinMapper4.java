package com;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper4 extends Mapper<Object, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] separatedInput = value.toString().split(";");

		String userID = separatedInput[0].trim().replaceAll("\"", "");

		if (userID == null) {
			return;
		}
		
		outKey.set(userID);
		outValue.set("D" + value);

		if (!userID.equals("ISBN")) {
			context.write(outKey, outValue);
		}
		
	}
}