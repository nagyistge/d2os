package watershed.core;

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import cloudos.kernel.DefaultExecutor;

public abstract class ChannelDeliver<DataType> extends DefaultExecutor {
	private ChannelReceiver<DataType> receiver;
	private int instance;
	private int nInstances;

	private String channelName;
	private Filter filter;

	private Map<String, String> attrs;

	public ChannelReceiver<DataType> getChannelReceiver(){
		return this.receiver;
	}

	public void setChannelReceiver(ChannelReceiver<DataType> receiver){
		this.receiver = receiver;
	}

	public void setInstance(int instance){
		this.instance = instance;
	}

	public int getInstance(){
		return this.instance;
	}

	public void setNumInstances(int nInstances){
		this.nInstances = nInstances;
	}

	public int getNumInstances(){
		return this.nInstances;
	}

	public void setChannelName(String channelName){
		this.channelName = channelName;
	}

	public String getChannelName(){
		return this.channelName;
	}

	public void deliver(String src, DataType data){
		getChannelReceiver().receive(src, data);
	}

	public void setFilter(Filter filter){
		this.filter = filter;
	}

	public Filter getFilter(){
		return this.filter;
	}

	public void halt(){
		filter.halt(getChannelName());
	}

	public void setAttribute(String key, String value){
		this.attrs.put(key, value);
	}

	public String getAttribute(String key){
		return this.attrs.get(key);
	}

	public Map<String, String> getAttributes(){
		return this.attrs;
	}


	public void setAttributes(Map<String, String> attrs){
		this.attrs = attrs;
	}


	//public abstract void start();
	//public abstract void finish();
}
