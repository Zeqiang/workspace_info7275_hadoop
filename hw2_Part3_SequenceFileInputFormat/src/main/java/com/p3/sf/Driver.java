package com.p3.sf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = Job.getInstance(conf);
		job.setJarByClass(countMapper.class);
		
		// Setup MapReduce
		job.setMapperClass(countMapper.class);
		job.setReducerClass(countReducer.class);
		job.setNumReduceTasks(0);
		
		// Specify key / value
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
//		job.setOutputKeyClass(LongWritable.class);
//		job.setOutputValueClass(Text.class);
		
		// Input
//		FileInputFormat.addInputPath(job, inputPath);
//		job.setInputFormatClass(TextInputFormat.class);
        
        SequenceFileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(SequenceFileInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

	}

}