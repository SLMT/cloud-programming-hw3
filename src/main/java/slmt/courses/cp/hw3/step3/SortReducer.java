package slmt.courses.cp.hw3.step3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SortReducer extends MapReduceBase implements
		Reducer<PageRank, NullWritable, PageRank, NullWritable> {

	public void reduce(PageRank inputKey, Iterator<NullWritable> inputVals,
			OutputCollector<PageRank, NullWritable> outputCollector, Reporter reporter)
			throws IOException {
		// Output the result
		outputCollector.collect(inputKey, NullWritable.get());
	}
}