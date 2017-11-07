package hw4.p4.grep404;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (args.length != 3) {
			System.err.println("Usage: DistributedGrep <regex> <in> <out>");
			System.exit(2);
		}
		
		String regex = new String(args[0]);
		Path inputPath = new Path(args[1]);
		Path outputDir = new Path("/Users/lzq/Documents/WORKSPACE/workspace_7275_HW/hw4_p4_DistributedGrep404web/tmpoutput");
		Path second_input_path = outputDir;
		Path final_output = new Path(args[2]);
		
		conf.set("Regex", regex);

		Job job = Job.getInstance(conf);
		job.setJobName("Distributed Grep");
		job.setJarByClass(DistributedGrepMapper.class);
		
		job.setMapperClass(DistributedGrepMapper.class);
		job.setCombinerClass(DistributedGrepCombiner.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		if (hdfs.exists(final_output))
			hdfs.delete(final_output, true);
		
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		boolean complete = job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "chaining uniqueIP");

		if (complete) {
			
			job2.setJarByClass(Driver.class);
			
			job2.setMapperClass(DistributedGrepMapper2.class);
			job2.setReducerClass(DistributedGrepReducer2.class);
			job2.setNumReduceTasks(1);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(NullWritable.class);
			
			job2.setOutputValueClass(Text.class);
			job2.setOutputKeyClass(NullWritable.class);

			FileInputFormat.addInputPath(job2, second_input_path);
			job2.setInputFormatClass(TextInputFormat.class);
			FileOutputFormat.setOutputPath(job2, final_output);

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}

}