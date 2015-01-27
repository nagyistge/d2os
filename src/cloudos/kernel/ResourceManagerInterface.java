package cloudos.kernel;

import java.util.Set;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.info.NodeInfo;

import cloudos.util.Json;

public class ResourceManagerInterface {
	private SystemCallInterface sysCall;

	public ResourceManagerInterface(SystemCallInterface sysCall){
		this.sysCall = sysCall;
	}
	
	//TODO do NOT through zookeeper exception, etc.
	public Environment getEnvironmentAlive() throws IOException, KeeperException, InterruptedException{
		SystemCallReply reply = sysCall.request("ResourceManager", "getEnvironmentAlive", null);
		Environment env = null;
		if(reply.getJSONValue()!=null){
			env = Json.loads(reply.getJSONValue(), Environment.class);
		}else if(reply.getErrorMessage()!=null){
			throw new SystemCallException(reply.getErrorMessage());
		}
		return env;
	}

	public Set<String> getMasterNames() throws IOException, KeeperException, InterruptedException {
		Environment env = getEnvironmentAlive();
		if(env==null)return null;
		else return env.getMasters();
	}

	public Set<String> getSlaveNames() throws IOException, KeeperException, InterruptedException {
		Environment env = getEnvironmentAlive();
		if(env==null)return null;
		else return env.getSlaves();
	}

	public void removeMaster(String nodeName) throws IOException, KeeperException, InterruptedException {
		SystemCallReply reply = sysCall.request("ResourceManager", "removeMaster", nodeName);
		if(reply.getErrorMessage()!=null){
			throw new SystemCallException(reply.getErrorMessage());
		}
	}

	public void removeSlave(String nodeName) throws IOException, KeeperException, InterruptedException {
		SystemCallReply reply = sysCall.request("ResourceManager", "removeSlave", nodeName);
		if(reply.getErrorMessage()!=null){
			throw new SystemCallException(reply.getErrorMessage());
		}
	}
}
