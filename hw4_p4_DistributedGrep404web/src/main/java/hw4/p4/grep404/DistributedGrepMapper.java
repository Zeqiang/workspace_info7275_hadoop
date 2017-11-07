package hw4.p4.grep404;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedGrepMapper extends Mapper<Object, Text, Text, Text>{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] ip = value.toString().split(" ");
		
		if(ip[7].contains("HTTP/") ) {
			
			Text t1 = new Text(ip[0]);
	        Text t2 = new Text(ip[8]);
	        
//	        System.err.println("Mapper: " + t1.toString() + "......" + t2.toString());
	        context.write(t1, t2);
			
		}
	}
}
