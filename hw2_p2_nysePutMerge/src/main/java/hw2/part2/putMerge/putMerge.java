package hw2.part2.putMerge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class putMerge {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if (args.length != 3) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <mergeFile path> <output path>");
			System.exit(-1);
		}

		// Create configuration
		Configuration conf = new Configuration(true);
		
		// Create hdfs
		FileSystem local = FileSystem.getLocal(conf);
		FileSystem hdfs = FileSystem.get(conf);
		
		Path inputDir = new Path(args[0]);
		Path hdfsFile = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		try {
			FileStatus[] inputFiles = local.listStatus(inputDir); // Get list of local Files
			
			// Delete mergeFile if exists
			if (hdfs.exists(hdfsFile)) {
				hdfs.delete(hdfsFile, true);
			}
			FSDataOutputStream out = hdfs.create(hdfsFile); // Create HDFS output stream
			
			for( int i=0;i < inputFiles.length; i++) {
				System.out.println(inputFiles[i].getPath().getName());
				FSDataInputStream in = local.open(inputFiles[i].getPath());
				
				byte buffer[] = new byte[256]; // Open local input stream
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0 ){
					out.write(buffer,0,bytesRead);
				}
				
				in.close();
			}

			out.close();
			
			
			// Create job
			Job job = Job.getInstance(conf);
			job.setJarByClass(nyseMapper.class);
			
			// Setup MapReduce
			job.setMapperClass(nyseMapper.class);
			job.setReducerClass(nyseReducer.class);
			job.setNumReduceTasks(1);
			
			// Specify key / value
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FloatWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			// Input
			FileInputFormat.addInputPath(job, hdfsFile);
			job.setInputFormatClass(TextInputFormat.class);

			// Output
			FileOutputFormat.setOutputPath(job, outputPath);

			// Delete output if exists
			FileSystem ifexist = FileSystem.get(conf);
			if (ifexist.exists(outputPath))
				ifexist.delete(outputPath, true);

			// Execute job
			int code = job.waitForCompletion(true) ? 0 : 1;
			System.exit(code);
			
			
		}catch(IOException e) {
			e.printStackTrace();
		}

	}

}