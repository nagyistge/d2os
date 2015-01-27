package dpm;

import java.math.BigInteger;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import org.apache.commons.configuration.Configuration;

import cloudos.kernel.SystemCallInterface;
import cloudos.kernel.info.ExecutorInfo;

import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;
import cloudos.kernel.SystemCallErrorType;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.SystemCallInterface;

import cloudos.util.Json;

public class ContainerController {
	private BigInteger processId;
	private SystemCallInterface sysCall;

	ContainerController(BigInteger processId, SystemCallInterface sysCall){
		this.processId = processId;
		this.sysCall = sysCall;
	}

	public void submitExecutor(Container container, ExecutorInfo executorInfo, Configuration conf) throws IOException{
		//SimpleProcess process = load from executorInfo 
		container.setExecutorInfo(executorInfo);
		container.setConfiguration(conf);
		SystemCallReply reply = this.sysCall.request(container.getNodeAddress(), "dpm", "runBasicProcess", container);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
	}

	public void killExecutor(Container container) throws IOException{
		SystemCallReply reply = this.sysCall.request(container.getNodeAddress(), "dpm", "kill", container); //<pid, cid>
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		container.setExecutorInfo(null);
	}

	public Container requestContainer() throws IOException, KeeperException, InterruptedException{ //TODO remove KeeperException dependency
		SystemCallReply reply = this.sysCall.request("dpm", "requestContainer", processId); //pid
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		Container container = Json.loads(reply.getJSONValue(), Container.class);
		return container;
	}
}

