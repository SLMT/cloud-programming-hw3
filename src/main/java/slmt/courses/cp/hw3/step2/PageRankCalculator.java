package slmt.courses.cp.hw3.step2;

import java.io.IOException;

public class PageRankCalculator {
	
	private static String TMP_DIR1 = "pageRankTmp1";
	private static String TMP_DIR2 = "pageRankTmp2";
	
	public static void main(String[] args) {
		try {
			
			String input = args[0];
			String output = args[1];
			new PageRankJob().run(input, output);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
