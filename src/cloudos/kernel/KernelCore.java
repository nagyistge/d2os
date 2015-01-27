package cloudos.kernel;

import java.io.IOException;

import java.io.DataOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.net.Socket;
import java.net.UnknownHostException;

import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

import org.apache.commons.lang.exception.NestableException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

import org.apache.zookeeper.KeeperException;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import cloudos.kernel.info.NodeInfo;
import cloudos.kernel.info.ModuleInfo;

import cloudos.util.ThreadExecutor;
import cloudos.util.Logger;
import cloudos.util.Json;

import cloudos.io.StandardOutputStream;

import cloudos.net.MessageHandler;
import cloudos.net.ConnectionHandler;
import cloudos.net.NetServer;
import cloudos.net.NetUtil;

public class KernelCore {

private static class SystemCallServer extends MessageHandler {
	private final ResourceManager resourceManager;
	private final ModuleController moduleController;

	SystemCallServer(ResourceManager resourceManager, ModuleController moduleController){
		this.resourceManager = resourceManager;
		this.moduleController = moduleController;
	}

	public void handleMessage(String msg){
		//Logger.info("Message received: "+msg);
		String json = StringUtils.newStringUtf8(Base64.decodeBase64(msg));
		SystemCallRequest sysCallMsg = Json.loads(json, SystemCallRequest.class);
		SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,"Unknown system call", SystemCallErrorType.FATAL);
		StandardOutputStream out = null;
		try{
			out = new StandardOutputStream(getSocket().getOutputStream());
		}catch(IOException e){
			e.printStackTrace();
		}
		//Logger.info("Message JSON: "+json);
		if(ResourceManager.class.getSimpleName().equals(sysCallMsg.getModule())){
			try{
				reply = this.resourceManager.receiveSystemCallRequest(sysCallMsg);
			}catch(IOException e){
				e.printStackTrace();
			}
		}else if(ModuleController.class.getSimpleName().equals(sysCallMsg.getModule())){
			try{
				reply = this.moduleController.receiveSystemCallRequest(sysCallMsg);
			}catch(IOException e){
				e.printStackTrace();
			}
		}else if(this.moduleController.getModuleNames().contains(sysCallMsg.getModule())){
			Logger.info("System Call: "+sysCallMsg.getModule()+" "+sysCallMsg.getMethod());
		}else{
			Logger.info("System Call Unknown module: "+sysCallMsg.getModule());
		}
		try{
			out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			out.flush();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}

	public static void clearZooKeeper(ZkClient zk){
		zk.deleteRecursive("/tmp");
		zk.deleteRecursive("/kernel");
		zk.deleteRecursive("/modules");
		/*
		try{
			if(zk.exists("/tmp", false)!=null)
				zk.deleteRecursive("/tmp");
			if(zk.exists("/kernel", false)!=null)
				zk.deleteRecursive("/kernel");
			if(zk.exists("/modules", false)!=null)
				zk.deleteRecursive("/modules");
		}catch(KeeperException e){
			e.printStackTrace();
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		*/
	}

	public static void main(String []args) throws IOException, ZkInterruptedException, KeeperException, InterruptedException, UnknownHostException, SAXException, SAXException, ParserConfigurationException{

		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt( "properties" )
                                .withDescription( "properties file name" )
                                .hasArg()
                                .withArgName("fileName")
                                .create("p"));
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try{
			cmd = parser.parse(options, args);
		}catch(ParseException e){
			e.printStackTrace();
		}
		String propFileName = "cloudos.properties";
		if(cmd.hasOption("p")){
			propFileName = cmd.getOptionValue("p");
		}
		Configuration config = null;

		try{
			config = new PropertiesConfiguration(propFileName);
		}catch(NestableException e){
			e.printStackTrace();
			System.exit(1);
		}
		Global.setConfiguration(config);
		
		Environment env = Environment.loadFromXMLFile(config.getString("env.file"));
		
		String localNodeName = NetUtil.getLocalHostName();
		NodeInfo localNodeInfo = env.getMasterInfo(localNodeName);
		if(localNodeInfo==null){
			throw new UnknownHostException("Host '"+localNodeName+"' not specified by the environment");
		}
		Global.setLocalNodeInfo(localNodeInfo);
		/*
		ZooKeeperClient zk = new ZooKeeperClient("localhost", 3000); //TODO set a default sessionTimeout
		*/
		ZkClient zk = new ZkClient("localhost");
		clearZooKeeper(zk);
		zk.createPersistent("/modules", "");
		zk.createPersistent("/tmp", "");
		zk.createPersistent("/kernel", ""); //TODO set that this host is the one that is running the kernelCore
		//zk.create("/kernel/addr", "192.168.0.103");
		zk.createPersistent("/kernel/addr", localNodeInfo.getAddress());
		zk.createPersistent("/kernel/port", config.getString("master.port"));
		//zk.close();
		String []zkServers = config.getStringArray("zk.servers");
		SystemCallInterface sysCall = new SystemCallInterface(zkServers);
		//System.out.println("zk.servers: "+Json.dumps(zkServers));

		String []modules = config.getStringArray("modules");

		final ResourceManager resourceManager = new ResourceManager(env, sysCall, zk);
		final ModuleController moduleController = new ModuleController(sysCall, zk);

		resourceManager.setModuleController(moduleController); //package-private method
		moduleController.setResourceManager(resourceManager); //package-private method
		
		//SystemCallServer.resourceManager = resourceManager;
		//SystemCallServer.moduleController = moduleController;

		//services.addThread("SystemCallServer", new NetServer<SystemCallServer>(config.getInt("master.port"), SystemCallServer.class) );
		ConnectionHandler.Factory factory = new ConnectionHandler.Factory() {
				public ConnectionHandler newConnectionHandler(){
					return new SystemCallServer(resourceManager, moduleController);
				}
		};

		ThreadExecutor<String, Thread> services = new ThreadExecutor<String, Thread>();
		try{
			NetServer<SystemCallServer> netServer = new NetServer<SystemCallServer>(config.getInt("master.port"), factory);
			services.addThread("SystemCallServer", netServer);
		}catch(IOException e){
			e.printStackTrace();
		}

		System.out.println("modules: "+Json.dumps(modules));
		if(Global.getLocalNodeInfo()!=null){
			System.out.println("LocalNodeInfo: "+Global.getLocalNodeInfo().getName());
		}
		
		for(String moduleFile: modules){
			ModuleInfo moduleInfo = ModuleInfo.loadFromXMLFile(moduleFile);
			moduleController.addModule(moduleInfo);
		}
		
	}
}
