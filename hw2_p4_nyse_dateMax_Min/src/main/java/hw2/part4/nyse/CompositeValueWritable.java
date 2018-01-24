package hw2.part4.nyse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeValueWritable implements Writable {

	private String date;
	private String volume;
	
	public CompositeValueWritable(){
		
	}

	public CompositeValueWritable(String date, String price) {
		super();
		this.date = date;
		this.volume = price;
	}

	public void write(DataOutput d) throws IOException {
		
		WritableUtils.writeString(d, date);
		WritableUtils.writeString(d, volume);
	}

	public void readFields(DataInput di) throws IOException {
		
		date = WritableUtils.readString(di);
		volume = WritableUtils.readString(di);
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getVolume() {
		return volume;
	}

	public void setVolume(String volume) {
		this.volume = volume;
	}

	public String toString(){
		return (new StringBuilder().append(date).append("\t").append(volume).toString());
	}
	
}
