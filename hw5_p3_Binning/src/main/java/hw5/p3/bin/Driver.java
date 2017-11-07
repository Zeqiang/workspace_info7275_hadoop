package hw5.p3.bin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		
		
		// String[] otherArgs = new GenericOptionsParser(conf, args)
		// .getRemainingArgs();

		if (args.length != 2) {
			System.err.println("Usage: Binning <input> <output>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();

		Path input = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Job job = Job.getInstance(conf,"Binning");
		job.setJarByClass(RequestMapper.class);

		job.setMapperClass(RequestMapper.class);
		job.setReducerClass(DistinctReducer.class);
		job.setNumReduceTasks(1);
		
		MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.setCountersEnabled(job, true);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}