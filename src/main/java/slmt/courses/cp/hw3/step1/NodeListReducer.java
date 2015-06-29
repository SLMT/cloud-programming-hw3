package slmt.courses.cp.hw3.step1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NodeListReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, NodeList> {

	public void reduce(Text inputKey, Iterator<Text> inputVals,
			OutputCollector<Text, NodeList> outputCollector, Reporter reporter)
			throws IOException {
		NodeList list = new NodeList();
		
		// Calculate the initial value of page rank
		long nodeCount = reporter.getCounter(NodeCounters.NODE_COUNTER).getValue();
		list.setPageRank(1.0 / (double) nodeCount);

		// Construct the node list
		while (inputVals.hasNext())
			list.addValue(inputVals.next().toString());

		// Output the result
		outputCollector.collect(inputKey, list);
	}
}