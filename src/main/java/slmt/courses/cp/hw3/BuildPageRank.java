package slmt.courses.cp.hw3;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.StringTokenizer;

import slmt.courses.cp.hw3.step1.phase1.ParsePagesJob;
import slmt.courses.cp.hw3.step1.phase2.BuildGraphJob;
import slmt.courses.cp.hw3.step2.PageRankJob;
import slmt.courses.cp.hw3.step3.SortJob;

public class BuildPageRank {

	private static String TMP_DIR = "pageRankTmp";
	private static String[] OUTPUT_FILES = new String[]{"part-00000", "part-00001"};
	
	public static void main(String[] args) {
		String inputDir = args[0];
		String outputDir = args[1];
		int iterationTimes = Integer.parseInt(args[2]);
		
		try {
			// Run the step 1
			ParsePagesJob.run(inputDir, outputDir);
			HDFS.replace(TMP_DIR, outputDir);
			BuildGraphJob.run(TMP_DIR, outputDir);
			System.out.println("[SLMT] The step 1 completed.");
			
			// Run the step 2
			for (int i = 1; i <= iterationTimes; i++) {
				HDFS.replace(TMP_DIR, outputDir);
				PageRankJob.run(TMP_DIR, outputDir);
				
				double distance = calculateDistance(outputDir);
				System.out.println("[SLMT] Iteration No." + i
						+ " completed. (Distance = " + distance + ")");
			}
			System.out.println("[SLMT] The step 2 completed.");
			
			// Run the step 3
			HDFS.replace(TMP_DIR, outputDir);
			SortJob.run(TMP_DIR, outputDir);
			System.out.println("[SLMT] The step 3 completed.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static double calculateDistance(String outputDir) throws IOException {
		double sum = 0.0;
		
		for (String outputFile : OUTPUT_FILES) {
			BufferedReader reader = HDFS.getBufferedReader(outputDir + "\\" + outputFile);
			String line = null;
			
			while ((line = reader.readLine()) != null && !line.isEmpty()) {
				StringTokenizer tokenizer = new StringTokenizer(line, "\t");
				
				// We only need the third token
				tokenizer.nextToken();
				tokenizer.nextToken();
				sum += Double.parseDouble(tokenizer.nextToken());
			}
			
			reader.close();
		}
		
		return sum;
	}
}
