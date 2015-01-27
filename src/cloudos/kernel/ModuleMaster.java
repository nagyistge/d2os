package cloudos.kernel;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import cloudos.kernel.info.NodeInfo;

import cloudos.util.Logger;
import cloudos.util.Json;

public abstract class ModuleMaster extends ModuleExecutor {
	private Environment environment;
	private Map<String, NodeInfo> nodes;

	private boolean firstLoad;
	public void start(){
		super.start();
		environment = new Environment();
		firstLoad = false;
		nodes = new HashMap<String, NodeInfo>();
		
	}

	public Environment getEnvironment(){
		if(!firstLoad){
			firstLoad = true;
			try{
				environment = (new ResourceManagerInterface(getSystemCallInterface())).getEnvironmentAlive();
			}catch(Exception e){
				Logger.warning("FAILED loading the alive environment");
				environment = new Environment();
				e.printStackTrace();
			}
		}
		return this.environment;
	}

	public void onSlaveDead(String nodeName){
		Logger.info("slave dead: "+nodeName);
		environment.removeSlave(nodeName);
	}
	
	public void onSlaveAlive(String nodeName){
		Logger.info("slave alive: "+nodeName);
		environment.addSlave(this.nodes.get(nodeName));
	}

	public void onMasterDead(String nodeName){
		Logger.info("master dead: "+nodeName);
		environment.removeMaster(nodeName);
	}
	
	public void onMasterAlive(String nodeName){
		Logger.info("master alive: "+nodeName);
		environment.addMaster(this.nodes.get(nodeName));
	}

	SystemCallReply receiveSystemCallRequest(SystemCallRequest sysCallMsg){
		if("onSlaveDead".equals(sysCallMsg.getMethod())){
			NodeInfo nodeInfo = Json.loads(sysCallMsg.getJSONValue(), NodeInfo.class);
			onSlaveDead(nodeInfo.getName());
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null);
		}else if("onSlaveAlive".equals(sysCallMsg.getMethod())){
			NodeInfo nodeInfo = Json.loads(sysCallMsg.getJSONValue(), NodeInfo.class);
			this.nodes.put(nodeInfo.getName(),nodeInfo);
			onSlaveAlive(nodeInfo.getName());
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null);
		}else if("onMasterDead".equals(sysCallMsg.getMethod())){
			NodeInfo nodeInfo = Json.loads(sysCallMsg.getJSONValue(), NodeInfo.class);
			onMasterDead(nodeInfo.getName());
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null);
		}else if("onMasterAlive".equals(sysCallMsg.getMethod())){
			NodeInfo nodeInfo = Json.loads(sysCallMsg.getJSONValue(), NodeInfo.class);
			this.nodes.put(nodeInfo.getName(),nodeInfo);
			onMasterAlive(nodeInfo.getName());
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(), null);
		}else{
			return handleSystemCall(sysCallMsg);
		}
	}
}
