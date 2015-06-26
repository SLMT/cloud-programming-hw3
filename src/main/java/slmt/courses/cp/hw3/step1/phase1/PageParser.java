package slmt.courses.cp.hw3.step1.phase1;

import java.util.HashSet;
import java.util.Set;

public class PageParser {
	
	// For debugging
	public static void main(String[] args) {
		PageParser p = new PageParser("<kerke><title>cat sam</title><haha>rer</haha><text>[[meow]]kfdsfe[[lala]]</text></kerke>");
		System.out.println(p.title);
		System.out.println(p.getLinks());
	}

	private String title;
	private Set<String> links = new HashSet<String>();

	public PageParser(String pageData) {
		parse(pageData);
	}

	public String getTitle() {
		return title;
	}

	public Set<String> getLinks() {
		return links;
	}

	private void parse(String pageData) {
		int searchStart = 0, searchEnd = 0, contentStart = 0, contentEnd = -1;

		// Retrieve the content of <title>
		contentStart = pageData.indexOf("<title>");
		if (contentStart == -1)
			throw new RuntimeException("something went wrong");
		contentStart += 7;
		contentEnd = pageData.indexOf("</title>");
		title = pageData.substring(contentStart, contentEnd);
		searchStart = contentEnd + 7;

		// Find the start index and the end index of <text>
		searchStart = pageData.indexOf("<text", searchStart);
		searchStart = pageData.indexOf(">", searchStart) + 1;
		searchEnd = pageData.indexOf("</text>", searchStart);

		// Retrieve all links
		while (searchStart < searchEnd
				&& (contentStart = pageData.indexOf("[[", searchStart)) != -1
				&& contentStart < searchEnd) {
			contentEnd = pageData.indexOf("]]", contentStart);

			// If there is no ']]' for this '[[', it must be at the end of text
			if (contentEnd == -1)
				break;

			links.add(pageData.substring(contentStart + 2, contentEnd));
			searchStart = contentEnd + 2;
		}
	}
}
