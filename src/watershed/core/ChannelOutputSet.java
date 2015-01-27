package watershed.core;

import java.io.IOException;

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import java.lang.reflect.ParameterizedType;

public class ChannelOutputSet<DataType>{

	private Map<String, ChannelSender<DataType>> senders;
	private boolean hasStarted;

	public ChannelOutputSet(){
		this.senders = new ConcurrentHashMap<String, ChannelSender<DataType>>();
		this.hasStarted = false;
	}

	public void addChannelSender(String filterName, ChannelSender<DataType> sender) {
		System.out.println("adding channel writer set: "+filterName);
		if(hasStarted){
			//System.out.println("starting channel writer set: "+channel.getHostName()+" "+channel.getTaskId());
			sender.start();
		}
		this.senders.put(filterName,sender);
	}

	public void start() {
		hasStarted = true;
		//this.hostName = hostName;
		//this.taskId = taskId;
		System.out.println("starting channel writer set");
		for(String filterName : this.senders.keySet()){
			this.senders.get(filterName).start();
			//ChannelWriter<DataType> output = outputChannels.get(filterName);
			//output.start(hostName, taskId);
		}
	}

	public Map<String, ChannelSender<DataType>> getChannelSenders(){
		return this.senders;
	}

	public ChannelSender<DataType> getChannelSender(String filterName){
		return this.senders.get(filterName);
	}

	public void send(DataType data) {
		//System.out.println("WRITER SET: writing in ("+outputChannels.keySet().size()+")");
		for(String filterName : this.senders.keySet()){
			//ChannelWriter<DataType> output = outputChannels.get(filterName);
			//output.write(data);
			this.senders.get(filterName).send(data);
		}
	}

	public void removeChannelSender(String filterName) {
		ChannelSender<DataType> sender = this.senders.remove(filterName);
		if(sender!=null) sender.finish();
	}

	public void finish() {
		for(String filterName : this.senders.keySet()){
			ChannelSender<DataType> sender = this.senders.remove(filterName);
			if(sender!=null) sender.finish();
		}
		
		hasStarted = false;
	}
}
