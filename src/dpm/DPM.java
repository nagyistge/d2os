package dpm;

import java.util.AbstractMap.SimpleEntry;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import cloudos.kernel.info.ExecutorInfo;

import cloudos.kernel.SystemCallInterface;
import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;

import cloudos.util.Json;

public class DPM{
	private SystemCallInterface sysCall;

	public DPM(SystemCallInterface sysCall){
		this.sysCall = sysCall;
	}

	public ProcessController submitProcess(ExecutorInfo executorInfo) throws IOException, KeeperException, InterruptedException {
		return submitProcess(executorInfo, new PropertiesConfiguration());
	}

	public ProcessController submitProcess(ExecutorInfo executorInfo, Configuration conf) throws IOException, KeeperException, InterruptedException {
		SimpleEntry<ExecutorInfo, Configuration> pair = new SimpleEntry<ExecutorInfo, Configuration>(executorInfo, conf);
		SystemCallReply reply = this.sysCall.request("dpm", "execute", pair);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		Container container = Json.loads(reply.getJSONValue(), Container.class);
		return new ProcessController(this.sysCall, container);
	}
}
