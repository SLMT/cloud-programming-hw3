package slmt.courses.cp.hw3.step1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class NodeList implements Writable {

	private Set<String> nodes;

	public NodeList() {
		this.nodes = new HashSet<String>();
	}

	public void addValue(String node) {
		nodes.add(node);
	}

	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		nodes = new HashSet<String>();
		for (int i = 0; i < len; i++)
			nodes.add(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(nodes.size());
		for (String value : nodes)
			out.writeUTF(value);
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		// Print the list
		for (String value : nodes) {
			sb.append(value);
			sb.append('\t');
		}
		
		return sb.substring(0, sb.length() - 1).toString();
	}
}
