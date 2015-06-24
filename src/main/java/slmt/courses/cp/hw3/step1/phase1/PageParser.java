package slmt.courses.cp.hw3.step1.phase1;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class PageParser {
	
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
		Document doc = parseXML(pageData);
		
		// Retrieve the content of <title>
		title = retrieveContent(doc, "title");
		
		// Retrieve all links
		String mainText = retrieveContent(doc, "text");
		int index = 0, start = -1, end = -1;
		while ((start = mainText.indexOf("[[", index)) != -1) {
			end = mainText.indexOf("]]", start);
			links.add(mainText.substring(start + 2, end));
			index = end + 2;
		}
	}
	
	private Document parseXML(String xml) {
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			return dBuilder.parse(new ByteArrayInputStream(xml
					.getBytes(StandardCharsets.UTF_8)));
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	private String retrieveContent(Document doc, String tagName) {
		NodeList nodeList = doc.getElementsByTagName(tagName);
		return nodeList.item(0).getTextContent();
	}
}
