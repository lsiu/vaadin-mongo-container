package com.github.lsiu.vaadin.mongocontainer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class GenerateSampleData {
	
	private MongoClient client;
	
	private DB db;
	
	public static void main(String[] args) {
		GenerateSampleData gen = new GenerateSampleData();
		gen.run();
	}
	
	public void run() {
		try {
			client = new MongoClient();
			db = client.getDB("test");
			
			convertXmlDataToJson();
		} catch (IOException | ParserConfigurationException | SAXException e) {
			throw new RuntimeException("RuntimeException. See cause.", e);
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	public void convertXmlDataToJson() throws IOException, ParserConfigurationException, SAXException {
		InputStream in =  new URL("http://data.one.gov.hk/dataset/22/en").openStream();
		
		processData(in);
		
		if (in != null) {
			in.close();
		}
	}

	private void processData(InputStream in) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
		InputSource inSource = new InputSource(in);
		Document doc = documentBuilder.parse(inSource);
		

		Element root = doc.getDocumentElement();
		root.normalize();
		NodeList nl = root.getElementsByTagName("LP");
		Gson gson = new Gson();
		DBCollection col = db.getCollection("restaurants");
	
		for (int i = 0; i < nl.getLength(); i++) {
			Node n = nl.item(i);
			
			if (n.getNodeType() == Node.ELEMENT_NODE) {
				BasicDBObject o = new BasicDBObject();
				
				Element e = (Element) n;

				String licenseNo = getTagValue("LICNO", e);
				o.put("_id", licenseNo);
				o.put("type", getTagValue("TYPE", e));
				o.put("districtCode", getTagValue("DIST", e));
				o.put("name", getTagValue("SS", e));
				o.put("address", getTagValue("ADR", e));
				o.put("info", getTagValue("INFO", e));
				
				System.out.println(gson.toJson(o));
				col.update(new BasicDBObject("_id", licenseNo), o, true, false);
			}
		}
		
	}

	private String getTagValue(String sTag, Element eElement) {
		NodeList nlList = eElement.getElementsByTagName(sTag).item(0)
				.getChildNodes();

		Node nValue = (Node) nlList.item(0);
		if (nValue == null)
			return null;
		return nValue.getNodeValue();
	}
}
