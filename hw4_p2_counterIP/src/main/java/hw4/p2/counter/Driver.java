package hw4.p2.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: CountIP <input> <output>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		
		conf.setInt(MRJobConfig.COUNTERS_MAX_KEY, 6000);
		Limits.init(conf);

		Path input = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Job job = Job.getInstance(conf);
		job.setJobName("Num of Access");
		job.setJarByClass(CountIPMapper.class);

		job.setMapperClass(CountIPMapper.class);
		job.setCombinerClass(CountIPCombiner.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		
		int code = job.waitForCompletion(true) ? 0 : 1;
		
		if (code == 0) {
			for (Counter counter : job.getCounters().getGroup(CountIPCombiner.IP_COUNTER_GROUP)) {
				System.err.println(counter.getDisplayName() + ": " + counter.getValue());
			}
		}
		
		System.exit(code);
		
		
	}
}