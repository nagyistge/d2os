package watershed.channel.net;

import java.io.IOException;

import java.net.Socket;
import java.net.ServerSocket;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.AbstractMap.SimpleEntry;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;


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

class KeyValueMessageDeliver extends MessageHandler {
	private ChannelDeliver<SimpleEntry<String, String>> deliver;

	public KeyValueMessageDeliver(ChannelDeliver<SimpleEntry<String, String>> deliver){
		this.deliver = deliver;
	}
	public void handleMessage(String msg){
		//Logger.info("Received: "+msg);
		SimpleEntry<String, String> data = Json.loads(msg, new TypeToken< SimpleEntry<String, String> >() {}.getType());
		//Logger.info("Delivering: "+data.toString());
		this.deliver.deliver("tmp", data);
	}
}

public class KeyValueDeliver extends ChannelDeliver<SimpleEntry<String, String>> implements ControlMessageReceiver {
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
				KeyValueMessageDeliver handler = new KeyValueMessageDeliver(this);
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
			//TODO evaluate this awaiting mechanism
			//serverExecutor.shutdown();
			/*try{
				while(!serverExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)){}
			}catch(InterruptedException e){
				e.printStackTrace();
			}*/
			halt();
		}
	}
}
