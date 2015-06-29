package slmt.courses.cp.hw3.step1.phase2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import slmt.courses.cp.hw3.step1.NodeCounters;
import slmt.courses.cp.hw3.step1.PageInfo;

public class BuildGraphMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable inputDocBaseOffset, Text inputText,
			OutputCollector<Text, Text> outputCollector, Reporter reporter)
			throws IOException {
		// Parse the output from the previous phase
		PageInfo page = new PageInfo(inputText.toString());
		
		// We only collect results when the title is in the node list
		if (page.hasSelfLink()) {
			Text titleText = new Text(page.getTitle());
			for (String node : page.getOutlinks())
				outputCollector.collect(new Text(node), titleText);
			
			// Increment the global counter
			reporter.getCounter(NodeCounters.NUM_NODES).increment(1);
		} else
			outputCollector.collect(new Text("No out link"), new Text(page.getTitle()));
	}
}
