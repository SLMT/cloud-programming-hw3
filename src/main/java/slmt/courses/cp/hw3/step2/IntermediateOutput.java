package slmt.courses.cp.hw3.step2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class IntermediateOutput implements Writable {
	
	public static enum DataType {
		NODE_LIST, PAGE_RANK
	}
	
	private DataType dataType;
	
	// For the node list
	private Set<String> outLinks;
	
	// For PageRank
	private double pageRank;
	
	public IntermediateOutput() {
	}
	
	public IntermediateOutput(Set<String> outLinks) {
		this.dataType = DataType.NODE_LIST;
		this.outLinks = outLinks;
	}
	
	public IntermediateOutput(double pageRank) {
		this.dataType = DataType.PAGE_RANK;
		this.pageRank = pageRank;
	}

	public void readFields(DataInput in) throws IOException {
		// Check the data type
		dataType = DataType.values()[in.readInt()];
		
		// Read the data
		if (dataType == DataType.NODE_LIST) {
			// Output the size of the list
			int size = in.readInt();
			outLinks = new HashSet<String>();
			
			// Output each node
			for (int i = 0; i < size; i++)
				outLinks.add(in.readUTF());
		} else if (dataType == DataType.PAGE_RANK) {
			pageRank = in.readDouble();
		}
	}

	public void write(DataOutput out) throws IOException {
		// Output the data type
		out.writeInt(dataType.ordinal());
		
		// Output the data
		if (dataType == DataType.NODE_LIST) {
			// Output the size of the list
			out.writeInt(outLinks.size());
			
			// Output each node
			for (String node : outLinks)
				out.writeUTF(node);
		} else if (dataType == DataType.PAGE_RANK) {
			out.writeDouble(pageRank);
		}
	}

	public DataType getDataType() {
		return dataType;
	}
	
	public Set<String> getOutLinks() {
		return outLinks;
	}
	
	public double getPageRank() {
		return pageRank;
	}
}
