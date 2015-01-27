package cloudos.kernel;

import java.io.IOException;

import java.net.UnknownHostException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;

import cloudos.kernel.info.NodeInfo;

import cloudos.io.StandardOutputStream;

import cloudos.net.NetUtil;

import cloudos.util.ThreadExecutor;
import cloudos.util.Logger;
import cloudos.util.Json;

class ResourceManager {
	private Environment liveEnv;
	private Environment deadEnv;
	private ThreadExecutor<String, NodeMonitor> masterMonitors;
	private ThreadExecutor<String, NodeMonitor> slaveMonitors;

	private ModuleController moduleController;
	private ZkClient zk;
	private SystemCallInterface sysCall;

	//TODO validate environment
	ResourceManager(Environment env, SystemCallInterface sysCall, ZkClient zk){
		this.liveEnv = new Environment();
		this.deadEnv = env;
		this.sysCall = sysCall;
		this.zk = zk;

		this.masterMonitors = new ThreadExecutor<String, NodeMonitor>();
		this.slaveMonitors = new ThreadExecutor<String, NodeMonitor>();

		for(String nodeName: env.getSlaves()){
			NodeInfo nodeInfo = env.getSlaveInfo(nodeName);
			this.slaveMonitors.addThread(nodeInfo.getName(), new NodeMonitor(nodeInfo, false, this, false));
		}
		for(String nodeName: env.getMasters()){
			NodeInfo nodeInfo = env.getMasterInfo(nodeName);
			this.masterMonitors.addThread(nodeInfo.getName(), new NodeMonitor(nodeInfo, true, this, false));
		}
	}

	/*ResourceManager(){
		this(new Environment());
	}*/

	Environment getEnvironmentAlive(){
		return this.liveEnv;
	}

	Environment getEnvironmentDead(){
		return this.deadEnv;
	}
	
	SystemCallInterface getSystemCallInterface(){
		return this.sysCall;
	}
	/*
	public void setSystemCallInterface(SystemCallInterface sysCall){
		this.sysCall = sysCall;
	}
	*/
	void setModuleController(ModuleController moduleController){
		this.moduleController = moduleController;
	}
	/*
	public void setZooKeeperClient(ZkClient zk){
		this.zk = zk;
	}
	*/
	synchronized void onSlaveDead(String nodeName){
		Logger.info("Slave dead: "+nodeName);
		NodeInfo nodeInfo = liveEnv.removeSlave(nodeName);
		if(nodeInfo!=null){
			deadEnv.addSlave(nodeInfo);
			//WARN ModuleController that this Node is no longer responding
			if(moduleController!=null) moduleController.onSlaveDead(nodeName);
		}
	}
	
	synchronized void onSlaveAlive(String nodeName){
		Logger.info("Slave alive: "+nodeName);
		NodeInfo nodeInfo = deadEnv.removeSlave(nodeName);
		if(nodeInfo!=null){
			liveEnv.addSlave(nodeInfo);
	
			String localNodeName = null;
			try{
				localNodeName = NetUtil.getLocalHostName();
			}catch(UnknownHostException e){
				e.printStackTrace();
			}
			NodeInfo localNodeInfo = liveEnv.getMasterInfo(localNodeName);

			//WARN ModuleController that this Node has became available
			//try{
				//NodeExecutor.setup(liveEnv.getSlaveInfo(nodeName), localNodeInfo);
				if(moduleController!=null) moduleController.onSlaveAlive(nodeName);
			//}catch(IOException e){
				//e.printStackTrace();
			//}
		}
	}

	synchronized void onMasterDead(String nodeName){
		Logger.info("Master dead: "+nodeName);
		NodeInfo nodeInfo = liveEnv.removeMaster(nodeName);
		if(nodeInfo!=null){
			deadEnv.addMaster(nodeInfo);
			//WARN ModuleController that this Node is no longer responding
			if(moduleController!=null) moduleController.onMasterDead(nodeName);
		}
	}
	
	synchronized void onMasterAlive(String nodeName){
		Logger.info("Master alive: "+nodeName);
		NodeInfo nodeInfo = deadEnv.removeMaster(nodeName);
		if(nodeInfo!=null){
			liveEnv.addMaster(nodeInfo);

			String localNodeName = null;
			try{
				localNodeName = NetUtil.getLocalHostName();
			}catch(UnknownHostException e){
				e.printStackTrace();
			}
			NodeInfo localNodeInfo = liveEnv.getMasterInfo(localNodeName);
	
			//WARN ModuleController that this Node has became available
			//try{
				//NodeExecutor.setup(liveEnv.getMasterInfo(nodeName), localNodeInfo);
				if(moduleController!=null) moduleController.onMasterAlive(nodeName);
			//}catch(IOException e){
				//e.printStackTrace();
			//}
		}
	}

	//SYSTEM CALLS

	void addSlave(NodeInfo nodeInfo){
		Logger.info("Adding slave: "+nodeInfo.getName()+" ("+nodeInfo.getAddress()+")");
		deadEnv.addSlave(nodeInfo);
		slaveMonitors.addThread(nodeInfo.getName(), new NodeMonitor(nodeInfo, false, this, false));
	}

	void addMaster(NodeInfo nodeInfo){
		Logger.info("Adding master: "+nodeInfo.getName()+" ("+nodeInfo.getAddress()+")");
		deadEnv.addMaster(nodeInfo);
		masterMonitors.addThread(nodeInfo.getName(), new NodeMonitor(nodeInfo, true, this, false));
	}

	void removeSlave(String nodeName){
		Logger.info("Removing slave: "+nodeName);
		slaveMonitors.removeThread(nodeName);
		deadEnv.removeSlave(nodeName);
		if(liveEnv.removeSlave(nodeName)!=null){
			onSlaveDead(nodeName);
		}
	}

	void removeMaster(String nodeName){
		Logger.info("Removing master: "+nodeName);
		masterMonitors.removeThread(nodeName);
		deadEnv.removeMaster(nodeName);
		if(liveEnv.removeMaster(nodeName)!=null){
			onMasterDead(nodeName);
		}
	}

	SystemCallReply receiveSystemCallRequest(SystemCallRequest sysCallMsg) throws IOException{
		if("addSlave".equals(sysCallMsg.getMethod())){
			NodeInfo nodeInfo = Json.loads(sysCallMsg.getJSONValue(), NodeInfo.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null);
			addSlave(nodeInfo);
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}else if("addMaster".equals(sysCallMsg.getMethod())){
			NodeInfo nodeInfo = Json.loads(sysCallMsg.getJSONValue(), NodeInfo.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null);
			addMaster(nodeInfo);
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}else if("removeSlave".equals(sysCallMsg.getMethod())){
			String nodeName = Json.loads(sysCallMsg.getJSONValue(), String.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null);
			removeSlave(nodeName);
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}else if("removeMaster".equals(sysCallMsg.getMethod())){
			String nodeName = Json.loads(sysCallMsg.getJSONValue(), String.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null);
			removeMaster(nodeName);
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}else if("getEnvironmentAlive".equals(sysCallMsg.getMethod())){
			String jsonValue = Json.dumps(getEnvironmentAlive());
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), jsonValue);
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
