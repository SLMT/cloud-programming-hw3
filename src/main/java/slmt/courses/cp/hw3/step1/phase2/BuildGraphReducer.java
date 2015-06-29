package slmt.courses.cp.hw3.step1.phase2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import slmt.courses.cp.hw3.PageInfo;

public class BuildGraphReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, PageInfo> {

	public void reduce(Text inputKey, Iterator<Text> inputVals,
			OutputCollector<Text, PageInfo> outputCollector, Reporter reporter)
			throws IOException {

		PageInfo info = new PageInfo();
		info.setTitle(inputKey.toString());
		info.setRank(1.0);

		// Construct the node list
		while (inputVals.hasNext())
			info.addOutLink(inputVals.next().toString());
		info.deleteSelfLink();

		// Output the result
		outputCollector.collect(inputKey, info);
	}
}