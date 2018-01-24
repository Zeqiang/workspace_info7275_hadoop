package hw4.p4.grep404;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedGrepMapper2 extends Mapper<Object, Text, Text, NullWritable>{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] ips = value.toString().split("\t");
		Text ip = new Text(ips[0]);
		context.write(ip, NullWritable.get());
	}
}
