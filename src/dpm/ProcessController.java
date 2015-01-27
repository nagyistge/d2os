package dpm;

import java.math.BigInteger;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.NodeAddress;

import cloudos.kernel.SystemCallInterface;
import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;

import cloudos.util.Json;

public class ProcessController {
	private Container container;
	private SystemCallInterface sysCall;

	ProcessController(SystemCallInterface sysCall, Container container){
		this.sysCall = sysCall;
		this.container = container;
	}
	
	NodeAddress getNodeAddress(){
		return this.container.getNodeAddress();
	}

	BigInteger getProcessId(){
		return this.container.getProcessId();
	}

	public void kill() throws IOException, KeeperException, InterruptedException {
		SystemCallReply reply = this.sysCall.request(container.getNodeAddress(), "dpm", "kill", container); //<pid, cid>
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		container.setExecutorInfo(null);
	}
}
