package hw5.p2.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		
		
		// String[] otherArgs = new GenericOptionsParser(conf, args)
		// .getRemainingArgs();

		if (args.length != 2) {
			System.err.println("Usage: CountNumUsersByState <input> <tmp_output>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();

		Path input = new Path(args[0]);
		Path outputDir = new Path(args[1]);
//		Path second_input_path = outputDir;
//		Path final_output = new Path(args[2]);

		Job job = Job.getInstance(conf,"Partitioning");
		job.setJarByClass(MonthMapper.class);

		job.setMapperClass(MonthMapper.class);
		job.setReducerClass(MonthReducer.class);
//		job.setCombinerClass(MonthReducer.class);
		job.setPartitionerClass(MonthPartitioner.class);
		job.setNumReduceTasks(12);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
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