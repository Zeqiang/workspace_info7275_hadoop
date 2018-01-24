package hw2.part5.topMovies;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,WritableComparable<CompositeKeyWritable> {

	private String mivieID;
	private String rating;
	
	public CompositeKeyWritable(){
		
	}

	public CompositeKeyWritable(String mivieID, String rating) {
		super();
		this.mivieID = mivieID;
		this.rating = rating;
	}

	public int compareTo(CompositeKeyWritable ck) {
		
		Double r1 = Double.parseDouble(this.rating);
		Double r2 = Double.parseDouble(ck.rating);
		Double result = r1 - r2;
		
		if (result > 0) {
			return -1;
		}else if (result < 0) {
			return 1;
		}else {
			return this.mivieID.compareTo(ck.mivieID);
		}
	}

	public void write(DataOutput d) throws IOException {
		
		WritableUtils.writeString(d, mivieID);
		WritableUtils.writeString(d, rating);
		
//		d.writeUTF(zipcode);
//		d.writeUTF(bikeId);
	}

	public void readFields(DataInput di) throws IOException {
		
		mivieID = WritableUtils.readString(di);
		rating = WritableUtils.readString(di);
		
//		zipcode = di.readUTF();
//		bikeId = di.readUTF();
	}

	public String getMivieID() {
		return mivieID;
	}

	public void setMivieID(String mivieID) {
		this.mivieID = mivieID;
	}

	public String getRating() {
		return rating;
	}

	public void setRating(String rating) {
		this.rating = rating;
	}

	public String toString(){
		return (new StringBuilder().append(mivieID).append("\t").append(rating).toString());
	}
	
}
