package com.barchart.globexpacketloss;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.google.common.net.HostAndPort;

public final class CmeXmlConfig {

	private static final String INCREMENTAL_A = "IA";

	private static final String INCREMENTAL_B = "IB";

	private static final String INSTRUMENT_A = "NA";

	private static final String INSTRUMENT_B = "NB";

	private static final String SNAPSHOT_A = "SA";

	private static final String SNAPSHOT_B = "SB";

	private final XPath xpath;

	private final Document document;

	public CmeXmlConfig(Document document, XPath xPath) {
		this.document = document;
		this.xpath = xPath;
	}

	public String getChannelDescription(int channelId) throws Exception {
		return evaluate("/configuration/channel[@id=" + channelId + "]/@label");
	}

	public HostAndPort getIncrementalFeedA(int channelId) throws Exception {
		return getHostAndPort(channelId, channelId + INCREMENTAL_A);
	}

	public HostAndPort getIncrementalFeedB(int channelId) throws Exception {
		return getHostAndPort(channelId, channelId + INCREMENTAL_B);
	}

	public HostAndPort getInstrumentFeedA(int channelId) throws Exception {
		return getHostAndPort(channelId, channelId + INSTRUMENT_A);
	}

	public HostAndPort getInstrumentFeedB(int channelId) throws Exception {
		return getHostAndPort(channelId, channelId + INSTRUMENT_B);
	}

	public HostAndPort getSnapshotFeedA(int channelId) throws Exception {
		return getHostAndPort(channelId, channelId + SNAPSHOT_A);
	}

	public HostAndPort getSnapshotFeedB(int channelId) throws Exception {
		return getHostAndPort(channelId, channelId + SNAPSHOT_B);
	}

	private HostAndPort getHostAndPort(int channelId, String connectionId) throws Exception {
		String host = evaluate("/configuration/channel[@id=" + channelId + "]/connections/connection[@id='" + connectionId + "']/ip");
		String port = evaluate("/configuration/channel[@id=" + channelId + "]/connections/connection[@id='" + connectionId + "']/port");
		return HostAndPort.fromString(host + ":" + port);
	}

	private String evaluate(String expression) throws Exception {
		XPathExpression xPathExpression = xpath.compile(expression);
		String result = xPathExpression.evaluate(document);
		return result;
	}

	public static CmeXmlConfig parse(URL url) throws IOException, SAXException, ParserConfigurationException {
		try (InputStream is = url.openStream()) {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setValidating(false);
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(is);
			XPathFactory xPathFactory = XPathFactory.newInstance();
			XPath xPath = xPathFactory.newXPath();
			return new CmeXmlConfig(document, xPath);
		}
	}

	@Override
	public String toString() {
		return "CmeChannelConfig [document=" + document + "]";
	}

	
	
}
