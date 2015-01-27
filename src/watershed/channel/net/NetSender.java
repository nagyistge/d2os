package watershed.channel.net;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.Global;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.NodeCommunicator;
import cloudos.kernel.SystemCallInterface;

import cloudos.util.Logger;
import cloudos.util.Json;

import watershed.core.ControlMessageReceiver;
import watershed.core.ControlMessage;

import watershed.core.InstanceAddress;
import watershed.core.ChannelSender;

public abstract class NetSender<DataType> extends ChannelSender<DataType> implements ControlMessageReceiver {

	NodeCommunicator []comm = null;

	public void start(){
		super.start();
		comm = new NodeCommunicator[getDestinationInstances()];
		//ControlMessage ctrlMsg = new ControlMessage("/F1/out/net/F2", "/F2/in/net", "InstanceAddress");
		ControlMessage ctrlMsg = new ControlMessage("/"+getSourceFilterName()+"/out/"+getChannelName()+"/"+getDestinationFilterName()+":"+getInstance(),
												"/"+getDestinationFilterName()+"/in/"+getChannelName(), "InstanceAddress");
		
		try{
			getSystemCallInterface().request("watershed","sendCtrlMsg", ctrlMsg);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public void finish(){
		super.finish();
	}

	public NodeCommunicator getCommunicator(int index){
		if(index>=0 && comm!=null && index<comm.length){
			return comm[index];
		}else return null;
	}
	
	public void receiveCtrlMsg(ControlMessage ctrlMsg){
		Logger.info("NETSENDER: received ctrl msg: "+Json.dumps(ctrlMsg));
		InstanceAddress addr = Json.loads(ctrlMsg.getMessage(), InstanceAddress.class);
		Logger.info("Connecting to: "+addr.getNodeAddress().getNodeInfo().getName()+":"+addr.getNodeAddress().getPort()+" instance: "+addr.getInstance() );
		if(addr.getInstance()>=0 && addr.getInstance()<comm.length){
			comm[addr.getInstance()] = null;
			NodeCommunicator nodeComm = new NodeCommunicator(addr.getNodeAddress().getNodeInfo());
			try{
				nodeComm.connect(addr.getNodeAddress().getPort());
				comm[addr.getInstance()] = nodeComm;
				Logger.info("Connected to server");
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
}
