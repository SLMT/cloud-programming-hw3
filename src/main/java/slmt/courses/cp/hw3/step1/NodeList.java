package slmt.courses.cp.hw3.step1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class NodeList implements Writable {
	
	private double pageRank;
	private Set<String> nodes;

	public NodeList() {
		this.pageRank = 0.0;
		this.nodes = new HashSet<String>();
	}
	
	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}
	
	public double getPageRank() {
		return pageRank;
	}

	public void addValue(String node) {
		nodes.add(node);
	}

	public void readFields(DataInput in) throws IOException {
		// Page rank
		pageRank = in.readDouble();
		
		// Node list
		int len = in.readInt();
		nodes = new HashSet<String>();
		for (int i = 0; i < len; i++)
			nodes.add(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		// Page rank
		out.writeDouble(pageRank);
		
		// Node list
		out.writeInt(nodes.size());
		for (String value : nodes)
			out.writeUTF(value);
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		// Print the page rank
		sb.append(pageRank);
		sb.append('\t');
		
		// Print the list
		for (String value : nodes) {
			sb.append(value);
			sb.append('\t');
		}
		
		return sb.substring(0, sb.length() - 1).toString();
	}
}
