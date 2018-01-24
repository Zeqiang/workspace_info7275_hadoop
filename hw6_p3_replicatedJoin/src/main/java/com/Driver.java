package com;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver{

	public static void main(String[] args) throws Exception {
		
		if (args.length != 4) {
			System.err.println("Usage: Join <input1> <input2> <input3> <output>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration(true);
//		DistributedCache.addLocalFiles(conf, args[1]);
		Job job = Job.getInstance(conf, "innerJoin");
		job.setJarByClass(Driver.class);
		DistributedCache.addLocalFiles(job.getConfiguration(), args[1]);
		DistributedCache.addLocalFiles(job.getConfiguration(), args[2]);
//		job.addCacheFile(new Path(args[1]).toUri());

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[3]);
		
		job.setMapperClass(JoinMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		
		int success = job.waitForCompletion(true) ? 0 : 1;
		System.exit(success);
	}
}