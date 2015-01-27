package dpm;

import java.math.BigInteger;


import java.io.File;
import java.io.IOException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.AbstractMap.SimpleEntry;

import java.util.Set;
import java.util.TreeSet;

import java.util.AbstractMap.SimpleEntry;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import org.apache.zookeeper.KeeperException;

import org.apache.commons.configuration.Configuration;

import cloudos.kernel.info.ExecutorInfo;

import cloudos.kernel.ModuleMaster;
import cloudos.kernel.info.NodeInfo;
import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;
import cloudos.kernel.SystemCallErrorType;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.Global;
import cloudos.kernel.SystemCallInterface;

import cloudos.scheduler.RandomScheduler;

import cloudos.util.Json;
import cloudos.util.Logger;
import cloudos.util.TreeNode;

import cloudos.kernel.Scheduler;

import cloudos.scheduler.RandomScheduler;
import cloudos.scheduler.RoundRobinScheduler;

public class Master extends ModuleMaster{
	private BigInteger lastProcessId;
	private BigInteger lastContainerId;
	private Scheduler scheduler;

	public void start(){
		super.start();

		this.lastContainerId = BigInteger.ZERO;
		this.lastProcessId = BigInteger.ZERO;
		this.scheduler = new RoundRobinScheduler();
	}
	
	public void finish(){
		super.finish();
	}
	
	private Container requestContainer(BigInteger processId){
		this.lastContainerId = this.lastContainerId.add(BigInteger.ONE); //TODO reset lastContainerId (circular id)
		String nodeName = this.scheduler.nextNodeName(getEnvironment().getSlaves()); //TODO improve node scheduling
		NodeInfo nodeInfo = getEnvironment().getSlaveInfo(nodeName);
		if(nodeInfo==null) Logger.warning("Null Node schduled"); //TODO throw exception

		NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
		Container container = new Container(processId, this.lastContainerId, nodeAddr);
		return container;
	}
	
	private Container execute(ExecutorInfo executorInfo, Configuration conf) throws IOException, KeeperException, InterruptedException {
			this.lastProcessId = this.lastProcessId.add(BigInteger.ONE); //TODO reset lastProcessId (circular id)
			Container container = requestContainer(this.lastProcessId);
			container.setExecutorInfo(executorInfo);
			container.setConfiguration(conf);
			Logger.info("container scheduled");
			SystemCallReply reply = getSystemCallInterface().request(container.getNodeAddress(), "dpm", "runMasterProcess", container);
			if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
			return container;
	}
	
	public SystemCallReply handleSystemCall(SystemCallRequest sysCallMsg){
		if("execute".equals(sysCallMsg.getMethod())){
			Logger.info("execuring process");
			//ExecutorInfo executorInfo = Json.loads(sysCallMsg.getJSONValue(), ExecutorInfo.class);
			SimpleEntry<ExecutorInfo, Configuration> pair = Json.loads(sysCallMsg.getJSONValue(),
				new TypeToken< SimpleEntry<ExecutorInfo, Configuration> >() {}.getType() );
			ExecutorInfo executorInfo = pair.getKey();
			Configuration conf = pair.getValue();
			try {
				Container container = execute(executorInfo, conf);
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),Json.dumps(container));
			}catch(Exception e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
		}else if("requestContainer".equals(sysCallMsg.getMethod())){
			Logger.info("scheduling container");
			BigInteger processId = Json.loads(sysCallMsg.getJSONValue(), BigInteger.class);
			try {
				Container container = requestContainer(processId);
				Logger.info("container scheduled");
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),Json.dumps(container));
			}catch(Exception e){ //TODO get no container scheduled error
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
		}
		return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,"System call unkown", SystemCallErrorType.FATAL);
	}
}
