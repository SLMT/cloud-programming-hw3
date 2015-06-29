package slmt.courses.cp.hw3.step1;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class PageInfo {

	private String title; // It will not be printed in toString()
	private double rank;
	private Set<String> outLinks;
	
	public PageInfo() {
		this.title = "";
		this.rank = 0.0;
		this.outLinks = new HashSet<String>();
	}

	/**
	 * It only accepts the output of reducer in this program.
	 * 
	 * @param text
	 *            the output text from the reducer
	 */
	public PageInfo(String text) {
		StringTokenizer tokenizer = new StringTokenizer(text);

		// The first token must be the title
		title = tokenizer.nextToken();

		// The second token must be the page rank which is 0.0 for now
		rank = Double.parseDouble(tokenizer.nextToken());

		// The rest of tokens are the linked nodes
		outLinks = new HashSet<String>();
		while (tokenizer.hasMoreTokens())
			outLinks.add(tokenizer.nextToken());
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getTitle() {
		return title;
	}
	
	public void setRank(double rank) {
		this.rank = rank;
	}
	
	public double getRank() {
		return rank;
	}
	
	public void addOutLink(String link) {
		outLinks.add(link);
	}
	
	public Set<String> getOutlinks() {
		return outLinks;
	}
	
	public boolean hasSelfLink() {
		return outLinks.contains(title);
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		// Print the rank of the page
		sb.append(rank);
		sb.append('\t');
		
		// Print the out links of the page
		for (String link : outLinks) {
			sb.append(link);
			sb.append('\t');
		}
		
		return sb.substring(0, sb.length() - 1).toString();
	}
}