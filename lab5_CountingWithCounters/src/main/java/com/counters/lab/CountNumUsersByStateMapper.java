package com.counters.lab;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

//import mrdp.utils.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class CountNumUsersByStateMapper extends Mapper<Object, Text, NullWritable, NullWritable> {

	static JSONParser parser = new JSONParser();

	public static final String STATE_COUNTER_GROUP = "State_Counter";

	private String[] statesArray = new String[] { "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI",
			"ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
			"NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT", "VT", "VA", "WA",
			"WV", "WI", "WY" };

	private HashSet<String> states = new HashSet<String>(Arrays.asList(statesArray));

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		JSONObject city = null;
		try {
			city = (JSONObject) parser.parse(value.toString());
		} catch (ParseException e) {

			e.printStackTrace();
		}

		String state = (String) city.get("abbreviation");

		if (state != null && !state.isEmpty()) {
			boolean unknown = true;

			if (states.contains(state)) {

				// If so, increment the state's counter by 1 and flag it
				// as not unknown
				context.getCounter(STATE_COUNTER_GROUP, state).increment(1);
				unknown = false;
			}

			// If the state is unknown, increment the counter
			if (unknown) {
				context.getCounter(STATE_COUNTER_GROUP, "Unknown").increment(1);
			}
			
		} else {
			// If it is empty or null, increment the counter by 1
			context.getCounter(STATE_COUNTER_GROUP, "NullOrEmpty").increment(1);
		}
	}

}