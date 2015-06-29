package slmt.courses.cp.hw3.step3;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class SortJob {
	
	public static void main(String[] args) throws Exception {
		SortJob.run(args[0], args[1]);
	}
	
	public static void run(String inputPath, String outputPath) throws IOException {

		// Create a job configuration
		JobConf conf = new JobConf(SortJob.class);
		conf.setJobName("SLMT's Page Rank Job - Step 3 - Sorting Page Rank");

		// Set up mapping
		conf.setMapperClass(SortMapper.class);
		conf.setMapOutputKeyClass(PageRank.class);
		conf.setMapOutputValueClass(NullWritable.class);

		// Set up reducing
		conf.setReducerClass(SortReducer.class);
		conf.setOutputKeyClass(PageRank.class);
		conf.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumMapTasks(10);
		conf.setNumReduceTasks(1);

		JobClient.runJob(conf);
	}
	
}
