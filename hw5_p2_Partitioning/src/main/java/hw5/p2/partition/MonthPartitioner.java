package hw5.p2.partition;


import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.logging.log4j.core.config.plugins.util.ResolverUtil.Test;

public class MonthPartitioner extends Partitioner<Text, Text> implements Configurable {
	
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		
	}
	
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		
		int partition = 0;
		
		if(key.toString().equals("Jan")) {
			partition = 0;
		}else if(key.toString().equals("Feb")) {
			partition = 1;
		}else if(key.toString().equals("Mar")) {
			partition = 2;
		}else if(key.toString().equals("Apr")) {
			partition = 3;
		}else if(key.toString().equals("May")) {
			partition = 4;
		}else if(key.toString().equals("Jun")) {
			partition = 5;
		}else if(key.toString().equals("Jul")) {
			partition = 6;
		}else if(key.toString().equals("Aug")) {
			partition = 7;
		}else if(key.toString().equals("Sep")) {
			partition = 8;
		}else if(key.toString().equals("Oct")) {
			partition = 9;
		}else if(key.toString().equals("Nov")) {
			partition = 10;
		}else if(key.toString().equals("Dec")) {
			partition = 11;
		}
		
		// TODO Auto-generated method stub
		return partition;
	}
}
