package com.neshant.tutorial;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyComparator  extends WritableComparator {
		  protected NaturalKeyComparator() {
				super(FloatWritable.class, true);
			}

			@Override
			public int compare(WritableComparable w1, WritableComparable w2) {
				FloatWritable key1 = (FloatWritable) w1;
				FloatWritable key2 = (FloatWritable) w2;
				return	(key1.get()<key2.get() ? 1 :( key1==key2? 0 : -1));
				//descending
			}
		}

