package slmt.courses.cp.hw3.step1.phase1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ParsePagesMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable inputDocBaseOffset, Text inputText,
			OutputCollector<Text, Text> outputCollector, Reporter reporter)
			throws IOException {
		// Parse the document
		PageParser pageParser = new PageParser(inputText.toString());
		Text title = new Text(pageParser.getTitle());

		// Output a fake self link in order to 
		// identify the nodes that don't exist
		outputCollector.collect(title, title);
		
		// Output the nodes which this node links to
		for (String node : pageParser.getLinks())
			outputCollector.collect(new Text(node), title);
	}
}
