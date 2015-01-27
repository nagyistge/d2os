package watershed.core;

import java.io.IOException;

import java.util.Set;
import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import java.util.AbstractMap.SimpleEntry;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.DefaultExecutor;

import cloudos.util.Logger;

public abstract class Filter<InputType, OutputType> extends DefaultExecutor implements ChannelReceiver<InputType> {
	private String name;
	private int instance;
	private Map<String, String> attrs;
	private Set<String> inputChannels;
	private Map<String, Boolean> inChannelsHalted;

	private Set<String> outputChannels;
	private Map<String, ChannelOutputSet<OutputType>> outputSets;
	/*
	public Filter(){
		this.outputSets = new ConcurrentHashMap<String, ChannelOutputSet<OutputType>>();
	}
	*/
	//public abstract void start();
	//public abstract void finish();
	public abstract void process(String src, InputType data);

	public void receive(String src, InputType data){
		process(src, data);
	}

	public void setName(String name){
		this.name = name;
	}

	public String getName(){
		return this.name;
	}

	public void setInstance(int instance){
		this.instance = instance;
	}

	public int getInstance(){
		return this.instance;
	}

	public void setAttributes(Map<String, String> attrs){
		this.attrs = attrs;
	}

	public String getAttribute(String attr){
		return this.attrs.get(attr);
	}

	public void setInputChannels(Set<String> inputChannels){
		this.inputChannels = inputChannels;
		inChannelsHalted = new ConcurrentHashMap<String, Boolean>();
		for(String chann: this.inputChannels){
			inChannelsHalted.put(chann, Boolean.FALSE);
		}
	}
	
	public Set<String> getInputChannels(){
		return this.inputChannels;
	}

	public void setOutputChannels(Set<String> outputChannels){
		this.outputChannels = outputChannels;
		this.outputSets = new ConcurrentHashMap<String, ChannelOutputSet<OutputType>>();
		for(String channelName: this.outputChannels){
			this.outputSets.put(channelName, new ChannelOutputSet<OutputType>());
		}
	}

	public Set<String> getOutputChannels(){
		return this.outputChannels;
	}

	public ChannelOutputSet<OutputType> getOutputChannel(String channelName){
		return this.outputSets.get(channelName);
	}

	public void halt() throws IOException, KeeperException, InterruptedException {
		SimpleEntry<String, Integer> pair = new SimpleEntry<String,Integer>(getName(), new Integer(getInstance()));
		getSystemCallInterface().request("watershed", "haltFilterInstance", pair); //halt (name,instance)
	}

	public void halt(String channelName){
		if(inChannelsHalted.keySet().contains(channelName)){
			inChannelsHalted.put(channelName, Boolean.TRUE);

			onChannelHalt(channelName); //call event

			boolean allHalted = true;
			for(String chann: inChannelsHalted.keySet()){
				if(!inChannelsHalted.get(chann).booleanValue()){
					allHalted = false;
					break;
				}
			}
			if(allHalted) {
				onChannelsHalted(); //call event
			}
		}
	}

	public abstract void onChannelHalt(String channelName);

	public abstract void onChannelsHalted();
}
