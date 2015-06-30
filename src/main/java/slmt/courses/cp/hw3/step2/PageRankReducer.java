package slmt.courses.cp.hw3.step2;

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

import slmt.courses.cp.hw3.Counters;
import slmt.courses.cp.hw3.PageInfo;
import slmt.courses.cp.hw3.step2.IntermediateOutput.DataType;

public class PageRankReducer extends MapReduceBase implements
		Reducer<Text, IntermediateOutput, Text, PageInfo> {

	private static double RANDOM_JUMP_PROB = 0.15;
	
	private double danglingRanks;
	private long nodeCount;

	@Override
	public void configure(JobConf conf) {
		try {
			JobClient client = new JobClient(conf);
			RunningJob parentJob = client.getJob(JobID.forName(conf
					.get("mapred.job.id")));
			long counterVal = parentJob.getCounters().getCounter(
					Counters.DANGLING_RANKS);
			danglingRanks = PageInfo.scaleBackToDouble(counterVal);
			nodeCount = parentJob.getCounters().getCounter(
					Counters.NUM_NODES);
		} catch (IOException e) {
			danglingRanks = 0.0;
			nodeCount = -1;
			e.printStackTrace();
		}
	}

	public void reduce(Text inputKey, Iterator<IntermediateOutput> inputVals,
			OutputCollector<Text, PageInfo> outputCollector, Reporter reporter)
			throws IOException {

		// Initialize variables
		PageInfo info = new PageInfo();
		double rankSum = 0.0;
		double lastRank = 0.0;
		
		// Retrieve all results
		double secondTerm = 0.0;
		while (inputVals.hasNext()) {
			IntermediateOutput result = inputVals.next();
			
			if (result.getDataType() == DataType.NODE_LIST)
				info.addOutLink(result.getOutLinks());
			else if (result.getDataType() == DataType.PAGE_RANK)
				secondTerm += result.getPageRank();
			else if (result.getDataType() == DataType.LAST_RANK)
				lastRank = result.getPageRank();
		}
		
		// Calculate the first term
		rankSum += RANDOM_JUMP_PROB;
		
		// Calculate the second term
		rankSum += (1 - RANDOM_JUMP_PROB) * secondTerm;
		
		// Calculate the third term
		rankSum += (1 - RANDOM_JUMP_PROB) * (danglingRanks / nodeCount);

		// Save the rank
		info.setRank(rankSum);
		info.setRankDiff(Math.abs(rankSum - lastRank));
		
		// Debug: Accumulate total ranks
		reporter.getCounter(Counters.TOTAL_RANKS).increment(PageInfo.rescaleToLong(rankSum));
		
		// Output the result
		outputCollector.collect(inputKey, info);
	}
}