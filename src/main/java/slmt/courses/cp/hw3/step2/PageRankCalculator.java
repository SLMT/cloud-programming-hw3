package slmt.courses.cp.hw3.step2;

import java.io.IOException;

import slmt.courses.cp.hw3.HDFS;

public class PageRankCalculator {

	private static String TMP_DIR = "pageRankTmp";

	private static int INTERATION_TIMES = 15;

	public static void main(String[] args) {
		try {
			// In the first time, we use the first argument as the input
			// directory
			String input = args[0];
			String output = args[1];
			PageRankJob.run(input, output);
			
			// Then, we move the files from the output directory
			// to the tmp. directory and use tmp. dir. as the input dir.
			for (int i = 1; i < INTERATION_TIMES; i++) {
				HDFS.rmdir(TMP_DIR);
				HDFS.mv(output, TMP_DIR);
				
				PageRankJob.run(TMP_DIR, output);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
