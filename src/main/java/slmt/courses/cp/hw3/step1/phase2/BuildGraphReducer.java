package slmt.courses.cp.hw3.step1.phase2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

import slmt.courses.cp.hw3.NodeCounters;
import slmt.courses.cp.hw3.PageInfo;

public class BuildGraphReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, PageInfo> {

	private long nodeCount;

	@Override
	public void configure(JobConf conf) {
		try {
			JobClient client = new JobClient(conf);
			RunningJob parentJob = client.getJob(JobID.forName(conf
					.get("mapred.job.id")));
			nodeCount = parentJob.getCounters().getCounter(
					NodeCounters.NUM_NODES);
		} catch (IOException e) {
			nodeCount = -1;
			e.printStackTrace();
		}
	}

	public void reduce(Text inputKey, Iterator<Text> inputVals,
			OutputCollector<Text, PageInfo> outputCollector, Reporter reporter)
			throws IOException {

		PageInfo info = new PageInfo();

		// Calculate the initial value of PageRank
		info.setRank(1 / (double) nodeCount);

		// Construct the node list
		while (inputVals.hasNext())
			info.addOutLink(inputVals.next().toString());

		// Output the result
		outputCollector.collect(inputKey, info);
	}
}