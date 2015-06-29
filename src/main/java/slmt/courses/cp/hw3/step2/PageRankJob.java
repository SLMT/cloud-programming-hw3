package slmt.courses.cp.hw3.step2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import slmt.courses.cp.hw3.PageInfo;

public class PageRankJob {
	
	public static void run(String inputPath, String outputPath) throws IOException {

		// Create a job configuration
		JobConf conf = new JobConf(PageRankJob.class);
		conf.setJobName("SLMT's Page Rank Job - Step 2 - Calculating Page Rank");

		// Set up mapping
		conf.setMapperClass(PageRankMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntermediateOutput.class);

		// Set up reducing
		conf.setReducerClass(PageRankReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(PageInfo.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumMapTasks(10);
		conf.setNumReduceTasks(2);

		JobClient.runJob(conf);
	}
	
}
