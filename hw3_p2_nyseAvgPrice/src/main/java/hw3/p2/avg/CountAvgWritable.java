package hw3.p2.avg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountAvgWritable implements Writable{
	
	private long count;
	private double average;

	public CountAvgWritable() {
		super();
	}

	public CountAvgWritable(long count, double average) {
		super();
		this.count = count;
		this.average = average;
	}

	public void write(DataOutput out) throws IOException {
		
		out.writeLong(count);
		out.writeDouble(average);
	}

	public void readFields(DataInput in) throws IOException {
		
		count = in.readLong();
		average = in.readDouble();
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public double getAverage() {
		return average;
	}

	public void setAverage(double average) {
		this.average = average;
	}

	@Override
	public String toString() {
		return "count=" + count + ", average=" + average;
	}
}
