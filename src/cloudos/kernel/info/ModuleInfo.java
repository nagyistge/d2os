package cloudos.kernel.info;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import cloudos.util.Logger;


public class ModuleInfo {
	private String name;
	private ExecutorInfo masterExecutorInfo;
	private ExecutorInfo slaveExecutorInfo;
	private String masterNodeName;

	public ModuleInfo(String name, ExecutorInfo masterExecutorInfo, ExecutorInfo slaveExecutorInfo){
		this.name = name;
		this.masterExecutorInfo = masterExecutorInfo;
		this.slaveExecutorInfo = slaveExecutorInfo;
	}
	
	public void setName(String name){
		this.name = name;
	}

	public String getName(){
		return this.name;
	}

	public void setMasterExecutorInfo(ExecutorInfo masterExecutorInfo){
		this.masterExecutorInfo = masterExecutorInfo;
	}

	public ExecutorInfo getMasterExecutorInfo(){
		return this.masterExecutorInfo;
	}

	public void setSlaveExecutorInfo(ExecutorInfo slaveExecutorInfo){
		this.slaveExecutorInfo = slaveExecutorInfo;
	}

	public ExecutorInfo getSlaveExecutorInfo(){
		return this.slaveExecutorInfo;
	}

	public void setMasterNodeName(String masterNodeName){
		this.masterNodeName = masterNodeName;
	}

	public String getMasterNodeName(){
		return this.masterNodeName;
	}

	private static ExecutorInfo loadExecutorInfo(Element element) throws IOException{
		String className = null;
		String fileName = null;
		if(!"executor".equals(element.getTagName())) return null;
		NodeList nodes = element.getElementsByTagName("class");
		if(nodes.getLength()>0) className = nodes.item(0).getTextContent();
		nodes = element.getElementsByTagName("file");
		if(nodes.getLength()>0) fileName = nodes.item(0).getTextContent();
		File file = null;
		if(fileName!=null && fileName.trim().length()==0) fileName = null;
		else{
			file = new File(fileName);
		}
		
		ExecutorInfo executorInfo = new ExecutorInfo(className, file);
		return executorInfo;
	}

	public static ModuleInfo loadFromXMLFile(String xmlName) throws IOException{
		Logger.info("Loading module from "+xmlName);
		ModuleInfo moduleInfo=null;
		try {
			File fXmlFile = new File(xmlName);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
 
			//optional, but recommended
			//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			doc.getDocumentElement().normalize();

			if(!"module".equals(doc.getDocumentElement().getNodeName())){
				Logger.warning("Wrong document element name: "+doc.getDocumentElement().getNodeName());
			}

			String name = null;
			//String port = null;
			NodeList nodes = doc.getElementsByTagName("name");
			if(nodes.getLength()>0) name = nodes.item(0).getTextContent();
			//nodes = doc.getElementsByTagName("port");
			//if(nodes.getLength()>0) port = nodes.item(0).getTextContent();
			nodes = doc.getElementsByTagName("executor");
			moduleInfo = new ModuleInfo(name, null, null);
			for(int i = 0; i<nodes.getLength(); i++){
				Node node = nodes.item(i);
				if(node.getNodeType() == Node.ELEMENT_NODE){
					ExecutorInfo executorInfo = loadExecutorInfo((Element)node);
					String type = ((Element)node).getAttribute("type");
					if("master".equals(type)) moduleInfo.setMasterExecutorInfo(executorInfo);
					else if("slave".equals(type)) moduleInfo.setSlaveExecutorInfo(executorInfo);
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		return moduleInfo;
	}
}
