package cloudos.kernel;

import java.io.IOException;

import java.io.DataOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import java.net.Socket;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;

import cloudos.kernel.Global;
import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.NodeCommunicator;

import cloudos.kernel.info.NodeInfo;

import cloudos.util.Json;
import cloudos.util.Logger;

public class SystemCallInterface {
	//private String addr;
	//private int port;
	private ZkClient zk;
	private String []zkServers;
	private Stat stat;
	//private NodeCommunicator comm;
	private Map<String, String> moduleAddr;
	private Map<String, Integer> modulePort;
	//private Map<String, NodeCommunicator> comms;
	/*
	public SystemCallInterface(){
		this(Global.getMainNodeInfo(),
			Global.getConfiguration().getInt("master.port"));
	}
	*/
	public SystemCallInterface(String []zkServers) throws IOException, KeeperException, InterruptedException{
		//this.addr = addr;
		//this.port = port;
		this.zkServers = zkServers;
		this.zk = new ZkClient(zkServers[0]);
		//this.addr = new String(zk.getData("/kernel/addr",false,null));
		//this.port = Integer.parseInt(new String(zk.getData("/kernel/port",false,null)));
		//comm = new NodeCommunicator(this.addr);
		this.moduleAddr = new HashMap<String, String>();
		this.modulePort = new HashMap<String, Integer>();
		String kernelAddr = new String((String)zk.readData("/kernel/addr"));
		Integer kernelPort = Integer.parseInt(new String((String)zk.readData("/kernel/port")));
		this.moduleAddr.put("ResourceManager", kernelAddr);
		this.modulePort.put("ResourceManager", kernelPort);
		this.moduleAddr.put("ModuleController", kernelAddr);
		this.modulePort.put("ModuleController", kernelPort);
	}
	/*
	public SystemCallInterface(String addr, int port) throws IOException, KeeperException, InterruptedException{
		this.addr = addr;
		this.port = port;
		this.zk = new ZooKeeperClient(this.addr, 3000);
		comm = new NodeCommunicator(this.addr);
		
	}

	public SystemCallInterface(NodeInfo nodeInfo, int port) throws IOException, KeeperException, InterruptedException{
		this(nodeInfo.getAddress(), port);
	}

	public SystemCallInterface(NodeAddress nodeAddr) throws IOException, KeeperException, InterruptedException{
		this(nodeAddr.getNodeInfo().getAddress(), nodeAddr.getPort());
	}
	*/
	public <T> SystemCallReply request(String moduleName, String method, T obj) throws IOException, KeeperException, InterruptedException {
		return this.request(new SystemCallRequest(moduleName, method, Json.dumps(obj)));
	}
	/*
	public SystemCallReply requestTEMP(String moduleName, String method, Object ... args) throws IOException {
		List<String> params = new ArrayList<String>();
		for(Object obj: args){
			params.add(Json.dumps(obj));
		}
		return this.request(new SystemCallRequest(moduleName, method, Json.dumps(params)));
	}
	*/

	public SystemCallReply request(SystemCallRequest sysCallReq) throws IOException, KeeperException, InterruptedException {
		//Logger.info(this.addr+": "+this.port);
		if(!this.moduleAddr.keySet().contains(sysCallReq.getModule())){
			if(this.zk.getChildren("/modules").contains(sysCallReq.getModule())){
				String moduleAddr = new String((String)zk.readData("/modules/"+sysCallReq.getModule()+"/addr"));
				Integer modulePort = Integer.parseInt(new String((String)zk.readData("/modules/"+sysCallReq.getModule()+"/port")));
				this.moduleAddr.put(sysCallReq.getModule(), moduleAddr);
				this.modulePort.put(sysCallReq.getModule(), modulePort);
				//TODO start a watch for this module address
			}
		}
		NodeCommunicator comm = new NodeCommunicator(this.moduleAddr.get(sysCallReq.getModule()));
		Logger.info("comm: "+comm);
		Logger.info("modulePort: "+this.modulePort);
		Logger.info("sysCallReq: "+sysCallReq);
		Integer port = this.modulePort.get(sysCallReq.getModule());
		Logger.info("port: "+port);
		comm.connect(port.intValue());
		return request(comm, sysCallReq);
	}

	public <T> SystemCallReply request(NodeAddress nodeAddr, String moduleName, String method, T obj) throws IOException {
		return this.request(nodeAddr, new SystemCallRequest(moduleName, method, Json.dumps(obj)));
	}

	public SystemCallReply request(NodeAddress nodeAddr, SystemCallRequest sysCallReq) throws IOException {
		NodeCommunicator comm = new NodeCommunicator(nodeAddr.getNodeInfo().getAddress());
		comm.connect(nodeAddr.getPort());
		return request(comm, sysCallReq);
	}

	private SystemCallReply request(NodeCommunicator comm, SystemCallRequest sysCallReq) throws IOException {
		String json = Json.dumps(sysCallReq);
		String base64 = Base64.encodeBase64String(StringUtils.getBytesUtf8(json));
		SystemCallReply reply = null;
		
		comm.writeLine(base64);
		comm.flush();
		
		base64 = comm.readLine();
		json = StringUtils.newStringUtf8(Base64.decodeBase64(base64));
		reply = Json.loads(json, SystemCallReply.class);

		comm.close();
		return reply;
	}
}
