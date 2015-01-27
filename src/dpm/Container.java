package dpm;

import java.math.BigInteger;

import org.apache.commons.configuration.Configuration;

import cloudos.kernel.SystemCallInterface;
import cloudos.kernel.NodeAddress;

import cloudos.kernel.info.ExecutorInfo;

public class Container {
	private BigInteger processId;
	private BigInteger containerId;
	private NodeAddress nodeAddr;
	private ExecutorInfo executorInfo;
	private Configuration conf;

	Container(BigInteger processId, BigInteger containerId, NodeAddress nodeAddr){
		this.processId = processId;
		this.containerId = containerId;
		this.nodeAddr = nodeAddr;
		this.executorInfo = null;
	}
	
	NodeAddress getNodeAddress(){
		return this.nodeAddr;
	}

	BigInteger getId(){
		return this.containerId;
	}

	BigInteger getProcessId(){
		return this.processId;
	}

	void setExecutorInfo(ExecutorInfo executorInfo){
		this.executorInfo = executorInfo;
	}

	ExecutorInfo getExecutorInfo(){
		return this.executorInfo;
	}

	void setConfiguration(Configuration conf){
		this.conf = conf;
	}

	Configuration getConfiguration(){
		return this.conf;
	}
}

