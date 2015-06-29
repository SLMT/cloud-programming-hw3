package slmt.courses.cp.hw3.step1.phase1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import slmt.courses.cp.hw3.step1.PageInfo;

public class ParsePages {
	public static void main(String[] args) throws Exception {
		ParsePages wc = new ParsePages();
		wc.run(args[0], args[1]);
	}

	public void run(String inputPath, String outputPath) throws IOException {

		// Create a job configuration
		JobConf conf = new JobConf(ParsePages.class);
		conf.setJobName("SLMT's Page Rank Job - Step 1 - Phase 1 - Parse Pages");

		// Set up mapping
		conf.setMapperClass(ParsePagesMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		// Set up reducing
		conf.setReducerClass(ParsePagesReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(PageInfo.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumMapTasks(10);
		conf.setNumReduceTasks(2);

		JobClient.runJob(conf);
	}
}
