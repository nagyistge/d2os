package watershed.channel.net;

import java.io.IOException;

import java.net.Socket;
import java.net.ServerSocket;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.Global;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.SystemCallInterface;

import cloudos.util.Logger;
import cloudos.util.Json;

import cloudos.net.MessageHandler;

import watershed.core.ControlMessageReceiver;
import watershed.core.ControlMessage;

import watershed.core.InstanceAddress;
import watershed.core.ChannelDeliver;
import watershed.core.ChannelReceiver;

class MessageDeliver extends MessageHandler {
	private ChannelReceiver<String> receiver;

	public MessageDeliver(ChannelReceiver<String> receiver){
		this.receiver = receiver;
	}
	public void handleMessage(String msg){
		//Logger.info("Received: "+msg);
		this.receiver.receive("tmp", msg);
	}
}

public class NetDeliver extends ChannelDeliver<String> implements ControlMessageReceiver {
	private ExecutorService serverExecutor;
	private ServerSocket server;

	public void start(){
		super.start();
		serverExecutor = Executors.newCachedThreadPool();
		try{
			server = new ServerSocket(0);
		}catch(IOException e){
			e.printStackTrace();
		}
		while(true){
			try{
				MessageDeliver handler = new MessageDeliver(getChannelReceiver());
				handler.setSocket(this.server.accept());
				serverExecutor.execute( handler );
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}

	public void finish(){
		super.finish();
	}

	public void receiveCtrlMsg(ControlMessage ctrlMsg){
		Logger.info("NETDELIVER: received ctrl msg: "+Json.dumps(ctrlMsg));
		if("InstanceAddress".equals(ctrlMsg.getMessage())){
			NodeAddress nodeAddr = new NodeAddress(Global.getLocalNodeInfo(), this.server.getLocalPort());
			InstanceAddress addr = new InstanceAddress(nodeAddr, getInstance());
			ControlMessage replyCtrlMsg = new ControlMessage(ctrlMsg.getDestination(), ctrlMsg.getSource(), Json.dumps(addr));
			try{
				getSystemCallInterface().request("watershed","sendCtrlMsg", replyCtrlMsg);
			}catch(Exception e){
				e.printStackTrace();
			}
		}else if("ProducersHalted".equals(ctrlMsg.getMessage())){
			halt();
		}
	}
}
