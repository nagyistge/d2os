package watershed.channel.net;

import java.io.IOException;

import java.util.AbstractMap.SimpleEntry;

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

public class KeyValueSender extends NetSender<SimpleEntry<String, String>> {

	public void send(SimpleEntry<String, String> data){
		int key = ((String)data.getKey()).hashCode()%getDestinationInstances();
		NodeCommunicator comm = getCommunicator(key);
		if(comm!=null){
			try{
				String json = Json.dumps(data);
				comm.writeLine(json);
				//Logger.info("sending: "+key+": "+json);
				//comm.flush();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
}
