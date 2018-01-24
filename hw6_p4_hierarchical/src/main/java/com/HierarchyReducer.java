package com;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

public class HierarchyReducer extends Reducer<Text, Text, Text, NullWritable> {

	private String title = null;
	private ArrayList<String> tags = new ArrayList<String>();
	private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		tags.clear();
		
		for (Text val : values) {
			
			String[] separatedvalues = val.toString().split("\\|");
			
			for(int i=0; i<separatedvalues.length; i++) {
				tags.add(separatedvalues[i]);
			}
		}
		
		title = key.toString();
		
		String titleWithTagChildren = nestElements(title, tags);
		context.write(new Text(titleWithTagChildren), NullWritable.get());
	}

	private String nestElements(String title, List<String> tags) {
		
		try {
			// Create the new document to build the XML
			DocumentBuilder bldr = dbf.newDocumentBuilder();
			Document doc = bldr.newDocument();
			
			// create parent node to document
			Element movieEl = doc.createElement("movie");
			movieEl.setAttribute("Title", title);
			
			// Add the movie element to the document
			doc.appendChild(movieEl);
			
			// For each tag, copy it to the "movie" node
			for(String tag : tags) {
				Element tagsEl = doc.createElement("tags");
				tagsEl.setAttribute("Tag", tag);
				
				// Add the tags to the movie element
				movieEl.appendChild(tagsEl);
			}
			
			// Transform the document into a String of XML and return
			return transformDocumentToString(doc);

		} catch (Exception e) {
			return null;
		}
	}

	private String transformDocumentToString(Document doc) {
		try {
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			StringWriter writer = new StringWriter();
			transformer.transform(new DOMSource(doc), new StreamResult(writer));
			// Replace all new line characters with an empty string to have
			// one record per line.
			return writer.getBuffer().toString().replaceAll("\n|\r", "");
		} catch (Exception e) {
			return null;
		}
	}
}
