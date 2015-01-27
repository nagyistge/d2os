package cloudos.kernel;

import java.util.Map;
import java.util.HashMap;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

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

import cloudos.io.StandardOutputStream;

import cloudos.kernel.info.ModuleExecutorInfo;
import cloudos.kernel.info.ExecutorInfo;
import cloudos.kernel.info.NodeInfo;

import cloudos.net.MessageHandler;
import cloudos.net.ConnectionHandler;

import cloudos.util.ThreadExecutor;
import cloudos.util.Logger;
import cloudos.util.Json;

import cloudos.net.NetServer;

public class NodeManager {

	private SystemCallInterface sysCall;
	private Map<String, ModuleExecutorInfo> executors;
	private Map<String, ExecutionController<ModuleExecutor>> executorThreads;

	public NodeManager(Configuration config) throws IOException, KeeperException, InterruptedException{
		this.executors = new HashMap<String, ModuleExecutorInfo>();
		this.executorThreads = new HashMap<String, ExecutionController<ModuleExecutor>>();
		Logger.info("resetting the NodeManager...");
		String []zkServers = config.getStringArray("zk.servers");

		this.sysCall = new SystemCallInterface(zkServers);
		System.out.println("zk.servers: "+Json.dumps(zkServers));
	}

	public void execute(ModuleExecutorInfo moduleExecutorInfo){
		this.executors.put(moduleExecutorInfo.getName(), moduleExecutorInfo);
		Logger.info("executing: "+moduleExecutorInfo.getName());
		this.executorThreads.put(moduleExecutorInfo.getName(), new ExecutionController<ModuleExecutor>(moduleExecutorInfo.getExecutorInfo()));
		ModuleExecutor executor = (ModuleExecutor)this.executorThreads.get(moduleExecutorInfo.getName()).getExecutor();
		//executor.setMasterInfo(moduleExecutorInfo.getMasterInfo());
		executor.setSystemCallInterface(this.sysCall);
		this.executorThreads.get(moduleExecutorInfo.getName()).start();
		Logger.info("DONE! executing: "+moduleExecutorInfo.getName());
	}

	public void stop(String moduleName){
		if(this.executorThreads.keySet().contains(moduleName)){
			this.executorThreads.get(moduleName).finish();
			this.executorThreads.remove(moduleName);
			this.executors.remove(moduleName);
		}
	}

	public SystemCallReply receiveSystemCallRequest(SystemCallRequest sysCallMsg) throws IOException{
		if("execute".equals(sysCallMsg.getMethod())){
			ModuleExecutorInfo moduleExecutorInfo = Json.loads(sysCallMsg.getJSONValue(), ModuleExecutorInfo.class);
			execute(moduleExecutorInfo);
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
		}else if("stop".equals(sysCallMsg.getMethod())){
			String moduleName = Json.loads(sysCallMsg.getJSONValue(), String.class);
			stop(moduleName);
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
		}else if("ping".equals(sysCallMsg.getMethod())){
			Logger.info("ping");
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),"pong");
		}else{
			Logger.warning("Unknown system call method: "+sysCallMsg.getMethod());
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,"Unknown system call method: "+sysCallMsg.getMethod(), SystemCallErrorType.FATAL);
		}
	}

	public SystemCallReply receiveModuleSystemCallRequest(SystemCallRequest sysCallMsg) throws IOException{
		ModuleExecutor executor = (ModuleExecutor)this.executorThreads.get(sysCallMsg.getModule()).getExecutor();
		return executor.receiveSystemCallRequest(sysCallMsg);
	}

	public boolean hasModule(String moduleName){
		return executors.keySet().contains(moduleName);
	}
	

private static class SystemCallServer extends MessageHandler {
	private NodeManager nodeManager;
	
	public SystemCallServer(NodeManager nodeManager){
		this.nodeManager = nodeManager;
	}

	public void handleMessage(String msg){
		String json = StringUtils.newStringUtf8(Base64.decodeBase64(msg));
		SystemCallRequest sysCallMsg = Json.loads(json, SystemCallRequest.class);
		StandardOutputStream out = null;
		SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,"Unknown system call",SystemCallErrorType.FATAL);
		try{
			out = new StandardOutputStream(getSocket().getOutputStream());
		}catch(IOException e){
			e.printStackTrace();
		}
		if(sysCallMsg.getModule().equals(NodeManager.class.getSimpleName())){
			try{
				reply = nodeManager.receiveSystemCallRequest(sysCallMsg);
			}catch(IOException e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(),SystemCallErrorType.FATAL);
				e.printStackTrace();
			}
		}else if(nodeManager.hasModule(sysCallMsg.getModule())){
			Logger.info("handling: "+sysCallMsg.getModule());
			try{
				reply = nodeManager.receiveModuleSystemCallRequest(sysCallMsg);
			}catch(IOException e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(),SystemCallErrorType.FATAL);
				e.printStackTrace();
			}
			Logger.info("done! handling: "+sysCallMsg.getModule());
		}else{
			Logger.warning("System Call Unknown module: "+sysCallMsg.getModule());
		}
		try{
			out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			out.flush();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}

	public static void main(String []args) throws IOException, KeeperException, InterruptedException{
		ThreadExecutor<String, Thread> services = new ThreadExecutor<String, Thread>();

		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt( "properties" )
                                .withDescription( "properties file name" )
                                .hasArg()
                                .withArgName("fileName")
                                .create("p"));
		options.addOption(OptionBuilder.withLongOpt( "main-node" )
                                .withDescription( "main node info in base 64 encoding" )
                                .hasArg()
                                .withArgName("base64")
                                .create("mn"));
		options.addOption(OptionBuilder.withLongOpt( "local-node" )
                                .withDescription( "local node info in base 64 encoding" )
                                .hasArg()
                                .withArgName("base64")
                                .create("ln"));
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

		String mainNodeBase64 = null;
		if(cmd.hasOption("mn")){
			mainNodeBase64 = cmd.getOptionValue("mn");
		}
		String json = StringUtils.newStringUtf8(Base64.decodeBase64(mainNodeBase64));
		NodeInfo mainNodeInfo = Json.loads(json, NodeInfo.class);
		Global.setMainNodeInfo(mainNodeInfo);

		String localNodeBase64 = null;
		if(cmd.hasOption("ln")){
			localNodeBase64 = cmd.getOptionValue("ln");
		}
		json = StringUtils.newStringUtf8(Base64.decodeBase64(localNodeBase64));
		NodeInfo localNodeInfo = Json.loads(json, NodeInfo.class);
		Global.setLocalNodeInfo(localNodeInfo);

		Configuration config = null;
		try{
			config = new PropertiesConfiguration(propFileName);
		}catch(NestableException e){
			e.printStackTrace();
			System.exit(1);
		}
		
		Global.setConfiguration(config);
		//SystemCallServer.nodeManager = new NodeManager(config);

		final NodeManager nodeManager = new NodeManager(config);
		ConnectionHandler.Factory factory = new ConnectionHandler.Factory() {
				public ConnectionHandler newConnectionHandler(){
					return new SystemCallServer(nodeManager);
				}
		};
		try{
			NetServer<SystemCallServer> netServer = new NetServer<SystemCallServer>(config.getInt("slave.port"), factory);
			services.addThread("SystemCallServer", netServer);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
