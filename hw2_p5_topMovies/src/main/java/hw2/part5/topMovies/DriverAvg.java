package hw2.part5.topMovies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DriverAvg {

	public static boolean avgDriver(String infile, String outfile) throws Exception {
		
//		if (args.length != 3) {
//			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
//			System.exit(-1);
//		}

		Path inputPath = new Path(infile);
		Path outputDir = new Path(outfile);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = Job.getInstance(conf);
		job.setJobName("AverageRating");
		job.setJarByClass(avgMapper.class);
//		job.setGroupingComparatorClass(GroupingComparator.class);
//		job.setPartitionerClass(NaturalKeyPartitioner.class);
//		job.setSortComparatorClass(SecondarySortCompKeySortComparator.class);

		// Setup MapReduce
		job.setMapperClass(avgMapper.class);
		job.setReducerClass(avgReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.getLocal(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		return job.waitForCompletion(true);

	}

}