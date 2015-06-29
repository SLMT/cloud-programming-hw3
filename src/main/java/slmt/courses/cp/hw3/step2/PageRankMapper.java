package slmt.courses.cp.hw3.step2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import slmt.courses.cp.hw3.Counters;
import slmt.courses.cp.hw3.PageInfo;

public class PageRankMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntermediateOutput> {

	public void map(LongWritable inputDocBaseOffset, Text inputText,
			OutputCollector<Text, IntermediateOutput> outputCollector,
			Reporter reporter) throws IOException {
		// Parse the output from the previous step
		PageInfo page = new PageInfo(inputText.toString());
		Text titleText = new Text(page.getTitle());

		// Send the node list
		outputCollector.collect(titleText,
				new IntermediateOutput(page.getOutlinks()));

		if (page.getOutlinks().isEmpty())
			// Record the page rank if it is a dangling node
			reporter.getCounter(Counters.DANGLING_RANKS).increment(
					PageInfo.rescaleToLong(page.getRank()));
		else {
			// Calculate the second term of the page rank
			double pageRank = page.getRank();
			pageRank /= page.getOutlinks().size();
			
			// Send the page rank to all linked nodes
			for (String node : page.getOutlinks())
				outputCollector.collect(new Text(node), new IntermediateOutput(
						pageRank));
		}

		// Increment the global counter
		reporter.getCounter(Counters.NUM_NODES).increment(1);
	}
}
