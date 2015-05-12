package d2os.kernel;
/**
 * @author Rodrigo Caetano O. ROCHA
 * @date 23 July 2013
 */

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;


import java.io.File;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import d2os.kernel.info.NodeInfo;
import d2os.kernel.info.SSHInfo;

import d2os.util.Logger;

public class Environment {
	private Map<String, NodeInfo> slaves;
	private Map<String, NodeInfo> masters;
	
	Environment(){
		slaves = new ConcurrentHashMap<String, NodeInfo>();
		masters = new ConcurrentHashMap<String, NodeInfo>();
	}

	/**
	Returns a unmodifiable Set that contains the name of the slave nodes.
	*/
	public Set<String> getSlaves(){
		return Collections.unmodifiableSet(slaves.keySet());
	}

	public NodeInfo getSlaveInfo(String name){
		return slaves.get(name);
	}
	
	NodeInfo removeSlave(String name){
		return slaves.remove(name);
	}

	void addSlave(NodeInfo nodeInfo){
		slaves.put(nodeInfo.getName(), nodeInfo);
	}

	/**
	Returns a unmodifiable Set that contains the name of the master nodes.
	*/
	public Set<String> getMasters(){
		return Collections.unmodifiableSet(masters.keySet());
	}

	public NodeInfo getMasterInfo(String name){
		return masters.get(name);
	}
	
	NodeInfo removeMaster(String name){
		return masters.remove(name);
	}

	void addMaster(NodeInfo nodeInfo){
		masters.put(nodeInfo.getName(), nodeInfo);
	}

	private static SSHInfo loadSSHInfo(Element element){
		String user = null;
		String password = null;
		String port = null;
		String identity = null;
		if(!"ssh".equals(element.getTagName())) return null;
		NodeList nodes = element.getElementsByTagName("user");
		if(nodes.getLength()>0) user = nodes.item(0).getTextContent();
		nodes = element.getElementsByTagName("password");
		if(nodes.getLength()>0) password = nodes.item(0).getTextContent();
		nodes = element.getElementsByTagName("port");
		if(nodes.getLength()>0) port = nodes.item(0).getTextContent();
		nodes = element.getElementsByTagName("identity");
		if(nodes.getLength()>0) identity = nodes.item(0).getTextContent();

		if(password!=null && password.trim().length()==0) password = null;
		if(identity!=null && identity.trim().length()==0) identity = null;
		if(port==null || port.trim().length()==0) port = "22";

		return (new SSHInfo.Builder(user))
			.password(password)
			.identity(identity)
			.port(Integer.parseInt(port))
			.build();
	}

	private static NodeInfo loadNodeInfo(Element element){
		String name = null;
		String address = null;
		String workspace = null;
		SSHInfo sshInfo = null;
		if(!"node".equals(element.getTagName())) return null;
		NodeList nodes = element.getElementsByTagName("name");
		if(nodes.getLength()>0) name = nodes.item(0).getTextContent();
		nodes = element.getElementsByTagName("address");
		if(nodes.getLength()>0) address = nodes.item(0).getTextContent();
		nodes = element.getElementsByTagName("workspace");
		if(nodes.getLength()>0) workspace = nodes.item(0).getTextContent();
		nodes = element.getElementsByTagName("ssh");
		if(nodes.getLength()>0) sshInfo = loadSSHInfo((Element)nodes.item(0));

		//NodeInfo nodeInfo = new NodeInfo(name, address, workspace, sshInfo);
		NodeInfo nodeInfo = new NodeInfo.Builder(name, address).workspace(workspace).sshInfo(sshInfo).build();
		return nodeInfo;
	}

	private static List<NodeInfo> loadNodeInfoList(Element element){
		List<NodeInfo> nodeInfoList = new ArrayList<NodeInfo>();
		NodeList nodes = element.getElementsByTagName("node");
		for(int i = 0; i < nodes.getLength(); i++){
			Node node = nodes.item(i);
			if(node.getNodeType() == Node.ELEMENT_NODE){
				NodeInfo nodeInfo = loadNodeInfo((Element)node);
				if(nodeInfo!=null)
					nodeInfoList.add(nodeInfo);
			}
		}
		return nodeInfoList;
	}

	static Environment loadFromXMLFile(String xmlName) throws IOException, ParserConfigurationException, SAXException{
		Logger.info("Loading settings from "+xmlName);
		Environment env = null;

		File fXmlFile = new File(xmlName);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);

		//optional, but recommended
		//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
		doc.getDocumentElement().normalize();

		if(!"environment".equals(doc.getDocumentElement().getNodeName())){
			Logger.warning("Wrong document element name: "+doc.getDocumentElement().getNodeName());
		}

		env = new Environment();
		NodeList nodes = doc.getElementsByTagName("master");
		for(int i = 0; i<nodes.getLength(); i++){
			Node node = nodes.item(i);
			if(node.getNodeType() == Node.ELEMENT_NODE){
				List<NodeInfo> list = loadNodeInfoList((Element)node);
				for(NodeInfo nodeInfo : list){
					env.addMaster(nodeInfo);
				}
			}
		}
		nodes = doc.getElementsByTagName("slave");
		for(int i = 0; i<nodes.getLength(); i++){
			Node node = nodes.item(i);
			if(node.getNodeType() == Node.ELEMENT_NODE){
				List<NodeInfo> list = loadNodeInfoList((Element)node);
				for(NodeInfo nodeInfo : list){
					env.addSlave(nodeInfo);
				}
			}
		}
		return env;
	}
}
