package com;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer2 extends Reducer<Text, Text, Text, Text> {

	private static final Text EMPTY_TEXT = new Text();
	private Text tmp = new Text();

	private ArrayList<Text> listC = new ArrayList<Text>();
	private ArrayList<Text> listD = new ArrayList<Text>();

	private String joinType = null;

	public void setup(Context context) {
		joinType = context.getConfiguration().get("join.type");
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		listC.clear();
		listD.clear();
		
		while (values.iterator().hasNext()) {
			
			tmp = values.iterator().next();

			if (tmp.charAt(0) == 'C') {
				// remove appended charachter from the start and add to list
				listC.add(new Text(tmp.toString().substring(1)));

			} else if (tmp.charAt(0) == 'D') {
				// remove appended charachter from the start and add to list
				listD.add(new Text(tmp.toString().substring(1)));
			}
		}

		executeJoinLogic(context);
	}

	private void executeJoinLogic(Context context) throws IOException, InterruptedException {
		
		if (joinType.equalsIgnoreCase("inner")) {
			
			if (!listC.isEmpty() && !listD.isEmpty()) {
				for (Text C : listC) {
					for (Text D : listD) {
						context.write(C, D);
					}
				}
			}
		}
		
		if (joinType.equalsIgnoreCase("leftOuter")) {
			
			for (Text A : listC) {
				if (!listD.isEmpty()) {
					for (Text B : listD) {
						context.write(A, B);
					}
				} else {
					context.write(A, EMPTY_TEXT);
				}
			}
		}
		
		if (joinType.equalsIgnoreCase("rightOuter")) {
			for (Text B : listD) {
				if (!listC.isEmpty()) {
					for (Text A : listC) {
						context.write(A, B);
					}
				} else {
					context.write(EMPTY_TEXT, B);
				}
			}
		}

		if (joinType.equalsIgnoreCase("fullouter")) {
			
			if (!listC.isEmpty()) {
				for (Text A : listC) {
					if (!listD.isEmpty()) {
						for (Text B : listD) {
							context.write(A, B);
						}
					} else {
						context.write(A, EMPTY_TEXT);
					}
				}
			} else {
				for (Text B : listD) {
					context.write(EMPTY_TEXT, B);
				}
			}
		}
		
		if (joinType.equalsIgnoreCase("anti")) {
			
			if (listC.isEmpty() ^ listD.isEmpty()) {
				for (Text A : listC) {
					context.write(A, EMPTY_TEXT);
				}
				for (Text B : listD) {
					context.write(EMPTY_TEXT, B);
				}
			}
		}
	}
	
}
