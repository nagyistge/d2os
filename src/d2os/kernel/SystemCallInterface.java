package d2os.kernel;

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


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.DefaultEventExecutorGroup; //for non-blocking event handler
import io.netty.handler.codec.serialization.ClassResolvers; //serialization
import io.netty.handler.codec.serialization.ObjectDecoder;  //serialization
import io.netty.handler.codec.serialization.ObjectEncoder;  //serialization
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler; //channel event handler

import d2os.kernel.SystemCallRequest;
import d2os.kernel.SystemCallReply;
import d2os.kernel.NodeAddress;
import d2os.kernel.NodeCommunicator;

import d2os.kernel.info.NodeInfo;

import d2os.util.Json;
import d2os.util.Logger;

public class SystemCallInterface {
	private ZkClient zk;
	private String []zkServers;
	private Stat stat;

	private Map<String, String> moduleAddr;
	private Map<String, Integer> modulePort;

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

	public SystemCallReply request(String moduleName, String method, SystemCallArguments args) throws IOException, KeeperException, InterruptedException {
		return this.request(new SystemCallRequest(moduleName, method, args));
	}

	public SystemCallReply request(SystemCallRequest sysCallReq) throws IOException, KeeperException, InterruptedException {
		//Logger.info(this.addr+": "+this.port);
		if(!this.moduleAddr.keySet().contains(sysCallReq.module())){
			if(this.zk.getChildren("/modules").contains(sysCallReq.module())){
				String moduleAddr = new String((String)zk.readData("/modules/"+sysCallReq.module()+"/addr"));
				Integer modulePort = Integer.parseInt(new String((String)zk.readData("/modules/"+sysCallReq.module()+"/port")));
				this.moduleAddr.put(sysCallReq.module(), moduleAddr);
				this.modulePort.put(sysCallReq.module(), modulePort);
				//TODO start a watch for this module address
			}
		}
		NodeCommunicator comm = new NodeCommunicator(this.moduleAddr.get(sysCallReq.module()));
		Logger.info("comm: "+comm);
		Logger.info("modulePort: "+this.modulePort);
		Logger.info("sysCallReq: "+sysCallReq);
		Integer port = this.modulePort.get(sysCallReq.module());
		Logger.info("port: "+port);
		comm.connect(port.intValue());
		return request(comm, sysCallReq);
	}

	public SystemCallReply request(NodeAddress nodeAddr, String moduleName, String method, SystemCallArguments args) throws IOException {
		return this.request(nodeAddr, new SystemCallRequest(moduleName, method, args));
	}

	public SystemCallReply request(NodeAddress nodeAddr, SystemCallRequest sysCallReq) throws IOException {
		NodeCommunicator comm = new NodeCommunicator(nodeAddr.getNodeInfo().getAddress());
		comm.connect(nodeAddr.getPort());
		return request(comm, sysCallReq);
	}

	public SystemCallFuture request2(final NodeAddress nodeAddr, final SystemCallRequest sysCallReq) throws InterruptedException, IOException {
		return new SystemCallFuture(nodeAddr, sysCallReq);
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
