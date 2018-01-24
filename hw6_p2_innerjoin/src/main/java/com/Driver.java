package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver{

	public static void main(String[] args) throws Exception {
		
		if (args.length != 5) {
			System.err.println("Usage: Join <input1> <input2> <input3> <tmp> <output>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "innerJoin1");
		job.setJarByClass(Driver.class);

		Path outputTmp = new Path(args[3]);
		Path outputDir = new Path(args[4]);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinMapper2.class);
		job.getConfiguration().set("join.type", "inner");

		job.setReducerClass(JoinReducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputTmp);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputTmp))
			hdfs.delete(outputTmp, true);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		boolean complete = job.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "innerjoin2");
		job2.setJarByClass(Driver.class);

		if (complete) {
			
			MultipleInputs.addInputPath(job2, outputTmp, TextInputFormat.class, JoinMapper3.class);
			MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, JoinMapper4.class);
			job2.getConfiguration().set("join.type", "inner");

			job2.setReducerClass(JoinReducer2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job2, outputDir);

			int success = job2.waitForCompletion(true) ? 0 : 1;
			System.exit(success);
		}
	}
}