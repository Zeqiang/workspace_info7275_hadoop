package hw3.p3.sdm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MedianStdDevWritable implements Writable{

	private String median;
	private String stdDev;
	
	public MedianStdDevWritable() {
		super();
	}

	public MedianStdDevWritable(String median, String stdDev) {
		super();
		this.median = median;
		this.stdDev = stdDev;
	}

	public void write(DataOutput out) throws IOException {
		
		WritableUtils.writeString(out, median);
		WritableUtils.writeString(out, stdDev);
	}

	public void readFields(DataInput in) throws IOException {
		
		median = WritableUtils.readString(in);
		stdDev = WritableUtils.readString(in);
	}

	public String getMedian() {
		return median;
	}

	public void setMedian(String median) {
		this.median = median;
	}

	public String getStdDev() {
		return stdDev;
	}

	public void setStdDev(String stdDev) {
		this.stdDev = stdDev;
	}

	@Override
	public String toString() {
		return "Median:" + this.getMedian() + ";" + "StdDev:" + this.getStdDev();
	}
}
