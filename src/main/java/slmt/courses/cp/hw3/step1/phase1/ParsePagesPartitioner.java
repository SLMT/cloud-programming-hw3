package slmt.courses.cp.hw3.step1.phase1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class ParsePagesPartitioner implements
		Partitioner<Text, Text> {
	
	private static final int NUM_OF_ALPHA = 26;

	public void configure(JobConf job) {
	}

	public int getPartition(Text key, Text value, int numPartitions) {
		// Determine the number of words per partition
		int wordPerPart = NUM_OF_ALPHA / numPartitions;
		if (NUM_OF_ALPHA % numPartitions != 0)
			wordPerPart++;
		
		// Determine which partition
		int firstCh = key.charAt(0);
		int num = firstCh - 'a';
		return num / wordPerPart;
	}
}
