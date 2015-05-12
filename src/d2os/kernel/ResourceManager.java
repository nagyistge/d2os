package d2os.kernel;

import java.io.IOException;

import java.net.UnknownHostException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;

import d2os.kernel.info.NodeInfo;

import d2os.io.StandardOutputStream;

import d2os.net.NetUtil;

import d2os.util.ThreadExecutor;
import d2os.util.Logger;
import d2os.util.Json;

class ResourceManager extends ModuleExecutor {
	private Environment liveEnv;
	private Environment deadEnv;
	//private ThreadExecutor<String, NodeMonitor> masterMonitors;
	//private ThreadExecutor<String, NodeMonitor> slaveMonitors;

	private ModuleController moduleController;
	private ZkClient zk;

	//TODO validate environment
	ResourceManager(Environment env, ZkClient zk){
		this.liveEnv = new Environment();
		this.deadEnv = env;
		this.zk = zk;

		//this.masterMonitors = new ThreadExecutor<String, NodeMonitor>();
		//this.slaveMonitors = new ThreadExecutor<String, NodeMonitor>();

		addSystemCallHandler("addSlave", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				NodeInfo nodeInfo = (NodeInfo)args.get("nodeInfo");
				addSlave(nodeInfo);
				return ctx.reply(null);
			}
		});
		addSystemCallHandler("addMaster", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				NodeInfo nodeInfo = (NodeInfo)args.get("nodeInfo");
				addMaster(nodeInfo);
				return ctx.reply(null);
			}
		});
		addSystemCallHandler("removeSlave", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				String nodeName = (String)args.get("nodeName");
				removeSlave(nodeName);
				return ctx.reply(null);
			}
		});
		addSystemCallHandler("removeMaster", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				String nodeName = (String)args.get("nodeName");
				removeMaster(nodeName);
				return ctx.reply(null);
			}
		});
		addSystemCallHandler("getEnvironmentAlive", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				return ctx.reply(getEnvironmentAlive());
			}
		});
		addSystemCallHandler("nodeHeartbeat", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				return ctx.reply(null);
			}
		});
		for(String nodeName: env.getSlaves()){
			NodeInfo nodeInfo = env.getSlaveInfo(nodeName);
			//this.slaveMonitors.addThread(nodeInfo.getName(), new NodeMonitor(nodeInfo, false, this, false));
		}
		for(String nodeName: env.getMasters()){
			NodeInfo nodeInfo = env.getMasterInfo(nodeName);
			//this.masterMonitors.addThread(nodeInfo.getName(), new NodeMonitor(nodeInfo, true, this, false));
		}
	}

	Environment getEnvironmentAlive(){
		return this.liveEnv;
	}

	Environment getEnvironmentDead(){
		return this.deadEnv;
	}

	void setModuleController(ModuleController moduleController){
		this.moduleController = moduleController;
	}

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
		//slaveMonitors.addThread(nodeInfo.getName(), new NodeMonitor(nodeInfo, false, this, false));
	}

	void addMaster(NodeInfo nodeInfo){
		Logger.info("Adding master: "+nodeInfo.getName()+" ("+nodeInfo.getAddress()+")");
		deadEnv.addMaster(nodeInfo);
		//masterMonitors.addThread(nodeInfo.getName(), new NodeMonitor(nodeInfo, true, this, false));
	}

	void removeSlave(String nodeName){
		Logger.info("Removing slave: "+nodeName);
		//slaveMonitors.removeThread(nodeName);
		deadEnv.removeSlave(nodeName);
		if(liveEnv.removeSlave(nodeName)!=null){
			onSlaveDead(nodeName);
		}
	}

	void removeMaster(String nodeName){
		Logger.info("Removing master: "+nodeName);
		//masterMonitors.removeThread(nodeName);
		deadEnv.removeMaster(nodeName);
		if(liveEnv.removeMaster(nodeName)!=null){
			onMasterDead(nodeName);
		}
	}

}
