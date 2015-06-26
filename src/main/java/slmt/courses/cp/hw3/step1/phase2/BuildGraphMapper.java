package slmt.courses.cp.hw3.step1.phase2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class BuildGraphMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable inputDocBaseOffset, Text inputText,
			OutputCollector<Text, Text> outputCollector, Reporter reporter)
			throws IOException {
		// Parse the output from the previous phase
		StringTokenizer tokenizer = new StringTokenizer(inputText.toString());
		
		// The first token must be the title
		String title = tokenizer.nextToken();
		
		// The rest of tokens are the linked nodes
		Set<String> nodes = new HashSet<String>(); 
		while (tokenizer.hasMoreTokens())
			nodes.add(tokenizer.nextToken());
		
		// We only collect results when the title is in the node list
		if (nodes.contains(title)) {
			Text titleText = new Text(title);
			for (String node : nodes)
				outputCollector.collect(new Text(node), titleText);
		} else
			outputCollector.collect(new Text("No out link"), new Text(title));
	}
}
