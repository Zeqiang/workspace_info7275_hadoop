package com.join.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author neshant
 *
 */
public class Lab_Join extends Configured implements Tool {

	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(new Configuration(), new Lab_Join(), args);
		} catch (Exception ex) {
			Logger.getLogger(Lab_Join.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public int run(String[] strings) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ReduceJoin");
		job.setJarByClass(Lab_Join.class);

		Path outputDir = new Path(strings[2]);

		MultipleInputs.addInputPath(job, new Path(strings[0]), TextInputFormat.class, JoinMapper1.class);
		MultipleInputs.addInputPath(job, new Path(strings[1]), TextInputFormat.class, JoinMapper2.class);
		job.getConfiguration().set("join.type", "leftOuter");

		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
	
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 2;
	}
}