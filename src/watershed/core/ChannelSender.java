package watershed.core;

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import cloudos.kernel.DefaultExecutor;

public abstract class ChannelSender<DataType> extends DefaultExecutor {
	private int instance;

	private String channelName;
	private String srcFilterName;
	private String dstFilterName;
	private int dstInstances;

	private Map<String, String> attrs;

	//public abstract void start();
	//public abstract void finish();
	public abstract void send(DataType data);

	public void setInstance(int instance){
		this.instance = instance;
	}

	public int getInstance(){
		return this.instance;
	}

	public void setChannelName(String channelName){
		this.channelName = channelName;
	}

	public String getChannelName(){
		return this.channelName;
	}

	public void setSourceFilterName(String srcFilterName){
		this.srcFilterName = srcFilterName;
	}

	public String getSourceFilterName(){
		return this.srcFilterName;
	}

	public void setDestinationFilterName(String dstFilterName){
		this.dstFilterName = dstFilterName;
	}

	public String getDestinationFilterName(){
		return this.dstFilterName;
	}

	public void setDestinationInstances(int dstInstances){
		this.dstInstances = dstInstances;
	}

	public int getDestinationInstances(){
		return this.dstInstances;
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

}
