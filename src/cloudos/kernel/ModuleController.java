package cloudos.kernel;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;


import java.nio.charset.Charset;

import java.net.UnknownHostException;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.io.FileUtils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import org.apache.zookeeper.KeeperException;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkException;

import cloudos.kernel.info.ModuleInfo;
import cloudos.kernel.info.NodeInfo;

import cloudos.scheduler.LessBusyScheduler;

import cloudos.net.NetUtil;

import cloudos.io.StandardOutputStream;

import cloudos.util.Logger;
import cloudos.util.Json;

class ModuleController {
	private ResourceManager resourceManager;
	private ZkClient zk;
	private SystemCallInterface sysCall;

	private Map<String, ModuleInfo> modules;

	ModuleController(SystemCallInterface sysCall, ZkClient zk){
		modules = new HashMap<String, ModuleInfo>();
		this.sysCall = sysCall;
		this.zk = zk;
	}
	
	SystemCallInterface getSystemCallInterface(){
		return this.sysCall;
	}

	void setResourceManager(ResourceManager resourceManager){
		this.resourceManager = resourceManager;
	}

	Set<String> getModuleNames(){
		return modules.keySet();
	}

	ModuleInfo getModuleInfo(String name){
		return modules.get(name);
	}
	
	void onSlaveDead(String nodeName){
		Logger.info("Slave dead: "+nodeName);
	}
	
	void onSlaveAlive(String nodeName){
		Logger.info("Slave alive: "+nodeName);
		Environment env = this.resourceManager.getEnvironmentAlive();
		NodeInfo nodeInfo = env.getSlaveInfo(nodeName);
		//LAUNCH MODULE ON SLAVES
		for(String moduleName: this.modules.keySet()){
			ModuleInfo moduleInfo = this.modules.get(moduleName);

			//Saving memory
			//TODO make sure that a localNodeInfo will be assigned
			/*
			String localNodeName = null;
			try{
				localNodeName = NetUtil.getLocalHostName();
			}catch(UnknownHostException e){
				e.printStackTrace();
			}
			NodeInfo localNodeInfo = env.getMasterInfo(localNodeName);
			*/
			NodeInfo localNodeInfo = Global.getLocalNodeInfo();
			try{
				String json = FileUtils.readFileToString(new File(localNodeInfo.getWorkspace()+"modules/"+moduleInfo.getName()+".info"), Charset.defaultCharset());
				moduleInfo = Json.loads(json, ModuleInfo.class);
			}catch(IOException e){
				e.printStackTrace();
			}

			NodeInfo masterInfo = env.getMasterInfo(moduleInfo.getMasterNodeName());
			
			try{
				Logger.info("executing on "+moduleName+" on slave "+nodeName);
				NodeExecutor.execute(this.sysCall, moduleName, moduleInfo.getSlaveExecutorInfo(), nodeInfo, masterInfo);
				Logger.info(moduleName+" is running on slave "+nodeName);
			}catch(IOException e){
				e.printStackTrace();
			}
			//Saving memory
			moduleInfo.setSlaveExecutorInfo(null);
			moduleInfo.setMasterExecutorInfo(null);
		}

	}

	void onMasterDead(String nodeName){
		Logger.info("Master dead: "+nodeName);
		Environment env = this.resourceManager.getEnvironmentAlive();

		if(env.getMasters().isEmpty())return;

		for(String moduleName: this.modules.keySet()){
			ModuleInfo moduleInfo = this.modules.get(moduleName);
			if(moduleInfo.getMasterNodeName().equals(nodeName)){
				//TODO restart the module master in another server
				//TODO warn the slaves of the new location of the module master
				zk.deleteRecursive("/modules/"+moduleInfo.getName());
				moduleInfo.setMasterNodeName(null);

				List<String> busy = new ArrayList<String>();
				for(String tmpModuleName:this.modules.keySet()){
					busy.add(this.modules.get(tmpModuleName).getMasterNodeName());
				}
		
				String masterNodeName = LessBusyScheduler.scheduleNext(env.getMasters(), busy);
				if(masterNodeName!=null){
					NodeInfo masterInfo = env.getMasterInfo(masterNodeName);
					//LAUNCH MODULE ON MASTER
					Logger.info("executing on master "+masterNodeName);
					try{
						NodeExecutor.execute(this.sysCall, moduleInfo.getName(), moduleInfo.getMasterExecutorInfo(), masterInfo, masterInfo);
						Logger.info("Creating znodes");
						zk.createPersistent("/modules/"+moduleInfo.getName(), masterInfo.getName());
						Logger.info("Creating znodes addr");
						zk.createPersistent("/modules/"+moduleInfo.getName()+"/addr", masterInfo.getAddress());
						Logger.info("Creating znodes port");
						zk.createPersistent("/modules/"+moduleInfo.getName()+"/port", Global.getConfiguration().getString("slave.port"));
						Logger.info("Finished znodes");					
						moduleInfo.setMasterNodeName(masterNodeName);
					}catch(IOException e){
						e.printStackTrace();
					}
					
				}
			}
		}
	}
	
	void onMasterAlive(String nodeName){
		Logger.info("Master alive: "+nodeName);
		
		Environment env = this.resourceManager.getEnvironmentAlive();
		NodeInfo nodeInfo = env.getMasterInfo(nodeName);
		if(nodeInfo==null){
			Logger.info("ERRRRRROR: NodeInfo is NULL");
			return;
		}
		//LAUNCH MODULE ON SLAVES
		for(String moduleName: this.modules.keySet()){
			ModuleInfo moduleInfo = this.modules.get(moduleName);
			if(moduleInfo.getMasterNodeName()==null){
				NodeInfo localNodeInfo = Global.getLocalNodeInfo();
				try{
					String json = FileUtils.readFileToString(new File(localNodeInfo.getWorkspace()+"modules/"+moduleInfo.getName()+".info"), Charset.defaultCharset());
					moduleInfo = Json.loads(json, ModuleInfo.class);
				}catch(IOException e){
					e.printStackTrace();
				}
				//LAUNCH MODULE ON MASTER
				Logger.info("executing on master "+nodeName);
				try{
					
					NodeExecutor.execute(this.sysCall, moduleInfo.getName(), moduleInfo.getMasterExecutorInfo(), nodeInfo, nodeInfo);
					Logger.info("Creating znodes");
					zk.createPersistent("/modules/"+moduleInfo.getName(), nodeInfo.getName());
					Logger.info("Creating znodes addr");
					zk.createPersistent("/modules/"+moduleInfo.getName()+"/addr", nodeInfo.getAddress());
					Logger.info("Creating znodes port");
					zk.createPersistent("/modules/"+moduleInfo.getName()+"/port", Global.getConfiguration().getString("slave.port"));
					Logger.info("Finished znodes");
					moduleInfo.setMasterNodeName(nodeName);
				}catch(IOException e){
					e.printStackTrace();
				}
				//Saving memory
				moduleInfo.setSlaveExecutorInfo(null);
				moduleInfo.setMasterExecutorInfo(null);
			}
		}
	}

	//SYSTEM CALLS
	void addModule(ModuleInfo moduleInfo) throws IOException, ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		Logger.info("Adding module: "+moduleInfo.getName());
		if(this.modules.keySet().contains(moduleInfo.getName())){
			throw new IllegalArgumentException("The module \""+moduleInfo.getName()+"\" is already loaded");
		}
		Environment env = this.resourceManager.getEnvironmentAlive();
		
		List<String> busy = new ArrayList<String>();
		for(String moduleName:this.modules.keySet()){
			busy.add(this.modules.get(moduleName).getMasterNodeName());
		}
		
		String nodeName = LessBusyScheduler.scheduleNext(env.getMasters(), busy);
		
		this.modules.put(moduleInfo.getName(), moduleInfo);
		moduleInfo.setMasterNodeName(nodeName);
		moduleInfo.getMasterExecutorInfo().setName(RandomStringUtils.randomAlphanumeric(32));
		moduleInfo.getSlaveExecutorInfo().setName(RandomStringUtils.randomAlphanumeric(32));
		NodeInfo masterInfo = null;
		if(nodeName!=null){
			masterInfo = env.getMasterInfo(nodeName);
			//LAUNCH MODULE ON MASTER
			Logger.info("executing on master "+nodeName);
			NodeExecutor.execute(this.sysCall, moduleInfo.getName(), moduleInfo.getMasterExecutorInfo(), masterInfo, masterInfo);
		}else Logger.info("There is no master online!");
		Logger.info("executing on slaves");
		for(String slaveName: env.getSlaves()){
			NodeInfo nodeInfo = env.getSlaveInfo(slaveName);
			//LAUNCH MODULE ON SLAVES
			Logger.info("executing on slave "+slaveName);
		
			NodeExecutor.execute(this.sysCall, moduleInfo.getName(), moduleInfo.getSlaveExecutorInfo(), nodeInfo, masterInfo);
		}
		//Saving memory
		//TODO make sure that a localNodeInfo will be assigned
		/*String localNodeName = null;
		try{
			localNodeName = NetUtil.getLocalHostName();
		}catch(UnknownHostException e){
			e.printStackTrace();
		}
		NodeInfo localNodeInfo = env.getMasterInfo(localNodeName);
		*/
		NodeInfo localNodeInfo = Global.getLocalNodeInfo();
		try{
			FileUtils.writeStringToFile(new File(localNodeInfo.getWorkspace()+"modules/"+moduleInfo.getName()+".info"), Json.dumps(moduleInfo), Charset.defaultCharset(), false);
			moduleInfo.setSlaveExecutorInfo(null);
			moduleInfo.setMasterExecutorInfo(null);
		}catch(IOException e){
			e.printStackTrace();
		}
		if(masterInfo!=null){
			//try{
			Logger.info("Creating znodes");
			zk.createPersistent("/modules/"+moduleInfo.getName(), masterInfo.getName());
			Logger.info("Creating znodes addr");
			zk.createPersistent("/modules/"+moduleInfo.getName()+"/addr", masterInfo.getAddress());
			Logger.info("Creating znodes port");
			zk.createPersistent("/modules/"+moduleInfo.getName()+"/port", Global.getConfiguration().getString("slave.port"));
			Logger.info("Finished znodes");
			/*}catch(KeeperException e){
				e.printStackTrace();
			}catch(InterruptedException e){
				e.printStackTrace();
			}*/
		}
	}

	public void removeModule(String moduleName) throws IllegalArgumentException{
		Logger.info("Removing module: "+moduleName);
		if(!this.modules.keySet().contains(moduleName)){
			throw new IllegalArgumentException("There is no module \""+moduleName+"\" loaded");
		}
		ModuleInfo moduleInfo = modules.remove(moduleName);

		Environment env = this.resourceManager.getEnvironmentAlive();

		NodeInfo masterNodeInfo = env.getMasterInfo(moduleInfo.getMasterNodeName());
		//stop module on master
		NodeExecutor.stop(moduleInfo.getName(), moduleInfo.getMasterExecutorInfo(), masterNodeInfo);

		for(String slaveName: env.getSlaves()){
			NodeInfo nodeInfo = env.getSlaveInfo(slaveName);
			//STOP MODULE ON SLAVES
			NodeExecutor.stop(moduleInfo.getName(), moduleInfo.getSlaveExecutorInfo(), nodeInfo);
		} 
	}


	NodeAddress getModuleSystemCall(String moduleName){
		ModuleInfo moduleInfo = getModuleInfo(moduleName);
		Environment env = this.resourceManager.getEnvironmentAlive();
		NodeInfo masterNodeInfo = env.getMasterInfo(moduleInfo.getMasterNodeName());
		return new NodeAddress(masterNodeInfo, Global.getConfiguration().getInt("slave.port"));
	}
	

	SystemCallReply receiveSystemCallRequest(SystemCallRequest sysCallMsg) throws IOException {
		if("addModule".equals(sysCallMsg.getMethod())){
			ModuleInfo moduleInfo = Json.loads(sysCallMsg.getJSONValue(), ModuleInfo.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null);
			try{
				addModule(moduleInfo);
			}catch(IllegalArgumentException e){
				//reply.setErrorMessage(e.getMessage());
				//reply.setErrorType(SystemCallErrorType.FATAL);
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null, e.getMessage(), SystemCallErrorType.FATAL);
			}catch(IOException e){
				//reply.setErrorMessage(e.getMessage());
				//reply.setErrorType(SystemCallErrorType.FATAL);
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null, e.getMessage(), SystemCallErrorType.FATAL);
			}
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}else if("removeModule".equals(sysCallMsg.getMethod())){
			String moduleName = Json.loads(sysCallMsg.getJSONValue(), String.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null);
			try{
				removeModule(moduleName);
			}catch(IllegalArgumentException e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null, e.getMessage(), SystemCallErrorType.FATAL);
				//reply.setErrorMessage(e.getMessage());
				//reply.setErrorType(SystemCallErrorType.FATAL);
			}
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}else if("getModuleNames".equals(sysCallMsg.getMethod())){
			String jsonValue = Json.dumps(getModuleNames());
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),jsonValue);
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}else if("getModuleInfo".equals(sysCallMsg.getMethod())){
			String moduleName = Json.loads(sysCallMsg.getJSONValue(), String.class);
			String jsonValue = Json.dumps(getModuleInfo(moduleName));
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),jsonValue);
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}else if("getModuleSystemCall".equals(sysCallMsg.getMethod())){
			String moduleName = Json.loads(sysCallMsg.getJSONValue(), String.class);
			String jsonValue = Json.dumps(getModuleSystemCall(moduleName));
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),jsonValue);
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}else{
			Logger.warning("Unknown system call method: "+sysCallMsg.getMethod());
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,"Unknown system call method: "+sysCallMsg.getMethod(), SystemCallErrorType.FATAL);
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}

	}
}
