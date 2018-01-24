package hw4.part3.nyse;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class nyseReducer2 extends Reducer<Text, Text, Text, Text> {

	private Text rank = new Text();
	private Text result = new Text();
    
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		
		for (Text val : values) {
			
			if(first) {
				first = false;
			}else {
				sb.append(", ");
			}
			
			sb.append(val.toString());
		}
		
		result.set(sb.toString());
		
		if(key.toString().equals("level1")) {
			rank.set("AVG stock_price_adj_close less than 10     :");
		}else if(key.toString().equals("level2")) {
			rank.set("AVG stock_price_adj_close between 10 and 20:");
		}else if(key.toString().equals("level3")) {
			rank.set("AVG stock_price_adj_close between 20 and 30:");
		}else if(key.toString().equals("level4")) {
			rank.set("AVG stock_price_adj_close between 30 and 40:");
		}else if(key.toString().equals("level5")) {
			rank.set("AVG stock_price_adj_close between 40 and 50:");
		}else if(key.toString().equals("level6")) {
			rank.set("AVG stock_price_adj_close between 50 and 60:");
		}else if(key.toString().equals("level7")) {
			rank.set("AVG stock_price_adj_close more than 60     :");
		}
		
		context.write(rank, result);
	}
}