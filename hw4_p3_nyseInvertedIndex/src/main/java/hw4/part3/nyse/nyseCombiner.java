package hw4.part3.nyse;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class nyseCombiner extends Reducer<Text, Text, Text, Text> {

	private Text rank = new Text();
    
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
		double sum = 0;
		int n = 0;
		
		for (Text val : values) {
			
			sum += Double.parseDouble(val.toString());
			n += 1;
		}
		
		double result = sum/n;
		
		if(result < 10.0) {
			rank.set("level1");
		}else if(result >= 10.0 && result < 20.0) {
			rank.set("level2");
		}else if(result >= 20.0 && result < 30.0) {
			rank.set("level3");
		}else if(result >= 30.0 && result < 40.0) {
			rank.set("level4");
		}else if(result >= 40.0 && result < 50.0) {
			rank.set("level5");
		}else if(result >= 50.0 && result < 60.0) {
			rank.set("level6");
		}else {
			rank.set("level7");
		}
		
		context.write(key, rank);
	}
}