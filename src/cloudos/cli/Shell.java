package cloudos.cli;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;

import java.util.Map;
import java.util.HashMap;

import java.net.MalformedURLException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import org.apache.commons.lang.exception.NestableException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.info.NodeInfo;
import cloudos.kernel.Global;
import cloudos.kernel.SystemCallInterface;

import cloudos.util.ClassUtil;
import cloudos.util.Logger;
import cloudos.util.Json;

public class Shell {
	private Configuration config;
	private Map<String, ShellApplicationInfo> applications;
	private SystemCallInterface sysCall;

	public Shell(Configuration config) throws IOException, KeeperException, InterruptedException{
		this.config = config;
		this.applications = new HashMap<String, ShellApplicationInfo>();

		//String []zkServers = config.getStringArray("zk.servers");
		String []zkServers = Global.getConfiguration().getStringArray("zk.servers");

		this.sysCall = new SystemCallInterface(zkServers);

		System.out.println("zk.servers: "+Json.dumps(zkServers));
	}

	private ShellApplicationInfo loadShellAppInfo(Element element){
		String name = null;
		String className = null;
		String fileName = null;
		if(!"app".equals(element.getTagName())) return null;
		NodeList nodes = element.getElementsByTagName("name");
		if(nodes.getLength()>0) name = nodes.item(0).getTextContent();
		nodes = element.getElementsByTagName("class");
		if(nodes.getLength()>0) className = nodes.item(0).getTextContent();
		nodes = element.getElementsByTagName("file");
		if(nodes.getLength()>0) fileName = nodes.item(0).getTextContent();
		if(fileName!=null && fileName.trim().length()==0) fileName = null;

		ShellApplicationInfo appInfo = new ShellApplicationInfo(name, className, fileName);
		return appInfo;
	}

	public void loadFromXMLFile(String xmlName){
		Logger.info("Loading apps from "+xmlName);
		try {
			File fXmlFile = new File(xmlName);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
 
			doc.getDocumentElement().normalize();

			if(!"shellapps".equals(doc.getDocumentElement().getNodeName())){
				Logger.warning("Wrong document element name: "+doc.getDocumentElement().getNodeName());
			}

			NodeList nodes = doc.getElementsByTagName("app");
			for(int i = 0; i<nodes.getLength(); i++){
				Node node = nodes.item(i);
				if(node.getNodeType() == Node.ELEMENT_NODE){
					ShellApplicationInfo appInfo = loadShellAppInfo((Element)node);
					this.applications.put(appInfo.getName(), appInfo);
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void runApplication(ShellApplicationInfo appInfo, String []args){
		ShellApplication app = null;
		try{
			app = (ShellApplication)ClassUtil.load(appInfo.getClassName(), appInfo.getFileName()).newInstance();
		}catch(MalformedURLException e){
			e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}catch(InstantiationException e){
			e.printStackTrace();
		}catch(IllegalAccessException e){
			e.printStackTrace();
		}
		if(app!=null){
			app.setSystemCallInterface(this.sysCall);
			app.run(args);
		}else System.out.println("Could not open the program: "+args[0]);
	}
	
	public void run(){
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		for(;;){
			String str = null;
			System.out.print("> ");
			try{
				str = in.readLine();
			}catch(IOException e){
				e.printStackTrace();
			}
			String []cmd = str.split("\\s");
			if(cmd.length==1 && "exit".equals(cmd[0])) break;
			else if(cmd.length>0){
				if(this.applications.keySet().contains(cmd[0])){
					runApplication(this.applications.get(cmd[0]), cmd);
				}else {
					System.out.println("Command not found: "+cmd[0]);
				}
			}
		}
	}
	
	public static void main(String []args) throws IOException, KeeperException, InterruptedException{
		String osPropFileName = "cloudos.properties";
		/*if(cmd.hasOption("p")){
			propFileName = cmd.getOptionValue("p");
		}*/
		Configuration osConfig = null;

		try{
			osConfig = new PropertiesConfiguration(osPropFileName);
		}catch(NestableException e){
			e.printStackTrace();
			System.exit(1);
		}
		Global.setConfiguration(osConfig);

		Configuration config = null;
		try{
			config = new PropertiesConfiguration("shell.properties");
		}catch(NestableException e){
			e.printStackTrace();
			System.exit(1);
		}

		//TODO get the server address from somewhere else (ZooKeeper kernel node)
		//NodeInfo mainNodeInfo = new NodeInfo("rcor","192.168.25.5");
		//Global.setMainNodeInfo(mainNodeInfo);
		Shell sh = new Shell(config);
		sh.loadFromXMLFile(config.getString("apps.file"));
		sh.run();
	}
}
