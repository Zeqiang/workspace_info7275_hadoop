package com;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class JoinMapper extends Mapper<Object, Text, Text, Text> {
	
	private HashMap<String,String> userInfoList = new HashMap<String,String>();
	private HashMap<String,String> bookInfoList = new HashMap<String,String>();
	
	private Text outValue = new Text();
//	private String joinType = null;
	
	public void setup(Context context) throws IOException, InterruptedException {
		
//		Path[] localPaths = context.getLocalCacheFiles();
		Path[] localPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
		System.err.println(localPaths[0].toString());
		
		BufferedReader br1 = new BufferedReader(new FileReader(localPaths[0].toString()));
		String line1 = null;
		while((line1 = br1.readLine()) != null) {
			
			String[] separatedInput = line1.toString().split(";");
			String userID = separatedInput[0].trim().replaceAll("\"", "");
			
			System.out.println("..."+line1);
			userInfoList.put(userID, line1);
		}
		System.err.println(userInfoList.size());
		
		BufferedReader br2 = new BufferedReader(new FileReader(localPaths[1].toString()));
		String line2 = null;
		while((line2 = br2.readLine()) != null) {
			
			String[] separatedInput = line2.toString().split(";");
			String ISBN = separatedInput[0].trim().replaceAll("\"", "");
			
			bookInfoList.put(ISBN, line2);
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] separatedInput = value.toString().split(";");
		String userID = separatedInput[0].trim().replaceAll("\"", "");
		String ISBN = separatedInput[1].trim().replaceAll("\"", "");
		
		String userInfo = userInfoList.get(userID);
		String bookInfo = bookInfoList.get(ISBN);
		
		if(userInfo != null && bookInfo != null) {
			outValue.set(userInfo + "\t" + bookInfo);
			context.write(value, outValue);
		}
	}
}


