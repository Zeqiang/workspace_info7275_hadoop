package hw2.part4.nyse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,WritableComparable<CompositeKeyWritable> {

	private String symbol;
	private String price;
	
	public CompositeKeyWritable(){
		
	}

	public CompositeKeyWritable(String symbol, String price) {
		super();
		this.symbol = symbol;
		this.price = price;
	}

	public int compareTo(CompositeKeyWritable ck) {
		
		Double r1 = Double.parseDouble(this.price);
		Double r2 = Double.parseDouble(ck.price);
		Double result = r1 - r2;
		
		if (result > 0) {
			return 1;
		}else if (result < 0) {
			return -1;
		}else {
			return this.symbol.compareTo(ck.symbol);
		}
	}

	public void write(DataOutput d) throws IOException {
		
		WritableUtils.writeString(d, symbol);
		WritableUtils.writeString(d, price);
	}

	public void readFields(DataInput di) throws IOException {
		
		symbol = WritableUtils.readString(di);
		price = WritableUtils.readString(di);
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public String getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = price;
	}

	public String toString(){
		return (new StringBuilder().append(symbol).append(" : ").append(price).toString());
	}
	
}
