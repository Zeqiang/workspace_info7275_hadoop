package com.join.lab;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

	private static final Text EMPTY_TEXT = new Text();
	private Text tmp = new Text();

	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();

	private String joinType = null;

	public void setup(Context context) {
		joinType = context.getConfiguration().get("join.type");
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		listA.clear();
		listB.clear();
		
		while (values.iterator().hasNext()) {
			
			tmp = values.iterator().next();

			if (tmp.charAt(0) == 'A') {
				// remove appended charachter from the start and add to list
				listA.add(new Text(tmp.toString().substring(1)));

			} else if (tmp.charAt(0) == 'B') {
				// remove appended charachter from the start and add to list
				listB.add(new Text(tmp.toString().substring(1)));
			}
		}

		executeJoinLogic(context);
	}

	private void executeJoinLogic(Context context) throws IOException, InterruptedException {
		
		if (joinType.equalsIgnoreCase("leftOuter")) {
			
			for (Text A : listA) {
				if (!listB.isEmpty()) {
					for (Text B : listB) {
						context.write(A, B);
					}
				} else {
					context.write(A, EMPTY_TEXT);
				}
			}
		} else if (joinType.equalsIgnoreCase("rightOuter")) {
			for (Text B : listB) {
				if (!listA.isEmpty()) {
					for (Text A : listA) {
						context.write(A, B);
					}
				} else {
					context.write(EMPTY_TEXT, B);
				}
			}
		} else if (joinType.equalsIgnoreCase("fullouter")) {
			
			if (!listA.isEmpty()) {
				for (Text A : listA) {
					if (!listB.isEmpty()) {
						for (Text B : listB) {
							context.write(A, B);
						}
					} else {
						context.write(A, EMPTY_TEXT);
					}
				}
			} else {
				for (Text B : listB) {
					context.write(EMPTY_TEXT, B);
				}
			}
		} else if (joinType.equalsIgnoreCase("anti")) {
			
			if (listA.isEmpty() ^ listB.isEmpty()) {
				for (Text A : listA) {
					context.write(A, EMPTY_TEXT);
				}
				for (Text B : listB) {
					context.write(EMPTY_TEXT, B);
				}
			}
		}
	}
	
}
