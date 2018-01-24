package com.secondarysorting.lab;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator{
	
	protected GroupingComparator() {
		
		super(CompositeKeyWritable.class,true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1,WritableComparable w2){
		
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getZipcode().compareTo(key2.getZipcode());
		
		return cmpResult;
	}

}
