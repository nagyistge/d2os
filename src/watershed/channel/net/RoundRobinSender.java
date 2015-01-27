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

public class RoundRobinSender extends NetSender<String> {
	private int nextComm = 0;
	
	public void send(String data){
		NodeCommunicator comm = getCommunicator(nextComm);
		if(comm!=null){
			try{
				comm.writeLine(data);
				comm.flush();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		nextComm++;
		if(nextComm>=getDestinationInstances()) nextComm = 0;
	}
}
