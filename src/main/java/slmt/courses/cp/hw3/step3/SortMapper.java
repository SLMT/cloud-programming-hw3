package slmt.courses.cp.hw3.step3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import slmt.courses.cp.hw3.PageInfo;

public class SortMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, PageRank, NullWritable> {

	public void map(LongWritable inputDocBaseOffset, Text inputText,
			OutputCollector<PageRank, NullWritable> outputCollector,
			Reporter reporter) throws IOException {
		// Parse the output from the previous step
		PageInfo page = new PageInfo(inputText.toString());
		outputCollector.collect(new PageRank(page.getTitle(), page.getRank()),
				NullWritable.get());
	}
}
