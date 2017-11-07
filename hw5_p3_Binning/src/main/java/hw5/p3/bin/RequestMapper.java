package hw5.p3.bin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RequestMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private MultipleOutputs<Text, NullWritable> mos = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		mos = new MultipleOutputs<Text, NullWritable>(context);
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] values = value.toString().split(" ");
		String request = values[5].trim().substring(1).trim();

		if (values.length == 10) {

			context.write(new Text(request), NullWritable.get());
		}

		if (request.equalsIgnoreCase("GET")) {
			mos.write("bins", value, NullWritable.get(), "GET");
		}else if (request.equalsIgnoreCase("POST")) {
			mos.write("bins", value, NullWritable.get(), "POST");
		}else if (request.equalsIgnoreCase("HEAD")) {
			mos.write("bins", value, NullWritable.get(), "HEAD");
		}else if (request.equalsIgnoreCase("PUT")) {
			mos.write("bins", value, NullWritable.get(), "PUT");
		}else if (request.equalsIgnoreCase("OPTIONS")) {
			mos.write("bins", value, NullWritable.get(), "OPTIONS");
		}else if (request.equalsIgnoreCase("SEARCH")) {
			mos.write("bins", value, NullWritable.get(), "SEARCH");
		}else if (request.equalsIgnoreCase("TRACE")) {
			mos.write("bins", value, NullWritable.get(), "TRACE");
		}else if (request.equalsIgnoreCase("BADMTHD")) {
			mos.write("bins", value, NullWritable.get(), "BADMTHD");
		}else if (request.equalsIgnoreCase("CONNECT")) {
			mos.write("bins", value, NullWritable.get(), "CONNECT");
		}else if (request.equalsIgnoreCase("OPTIONS")) {
			mos.write("bins", value, NullWritable.get(), "OPTIONS");
		}else if (request.equalsIgnoreCase("PROPFIND")) {
			mos.write("bins", value, NullWritable.get(), "PROPFIND");
		}else {
			mos.write("bins", value, NullWritable.get(), "Others");
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		mos.close();
	}
}