package slmt.courses.cp.hw3.step1.phase2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import slmt.courses.cp.hw3.step1.PageInfo;

public class BuildGraph {
	public static void main(String[] args) throws Exception {
		BuildGraph wc = new BuildGraph();
		wc.run(args[0], args[1]);
	}

	public void run(String inputPath, String outputPath) throws IOException {

		// Create a job configuration
		JobConf conf = new JobConf(BuildGraph.class);
		conf.setJobName("SLMT's Page Rank Job - Step 1 - Phase 2 - Build Graph");

		// Set up mapping
		conf.setMapperClass(BuildGraphMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		// Set up reducing
		conf.setReducerClass(BuildGraphReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(PageInfo.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumMapTasks(10);
		conf.setNumReduceTasks(2);

		JobClient.runJob(conf);
	}
}
