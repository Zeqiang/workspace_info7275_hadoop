package com.join.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper2 extends Mapper<Object, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] values = value.toString().split(",");
		String questionId = values[0].trim();
		
		if (questionId == null) {
			return;
		}
		
		outKey.set(questionId);	//QID
		outValue.set("B" + "," + values[1].trim());	//AnswerID
		
		
		if(!questionId.equals("Question ID")) {
			context.write(outKey, outValue);	
		}
	}
}