package slmt.courses.cp.hw3.step3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PageRank implements Writable, WritableComparable<PageRank> {

	private String name;
	private double rank;

	public PageRank() {
	}

	public PageRank(String name, double rank) {
		this.name = name;
		this.rank = rank;
	}

	public String getName() {
		return name;
	}

	public double getRank() {
		return rank;
	}

	public int compareTo(PageRank target) {
		if (target.rank < rank)
			return -1;
		
		if (target.rank > rank)
			return 1; 
		
		return name.compareTo(target.name);
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (!(obj instanceof PageRank))
			return false;
		
		PageRank pr = (PageRank) obj;
		
		if (rank - pr.rank < 0.0000001 && name.equals(pr.name))
			return true;
		
		return false;
	}

	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		rank = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeDouble(rank);
	}

	public String toString() {
		return name + "\t" + rank;
	}
}
